// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kubelet // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscontainerinsightreceiver/internal/kubelet"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	ci "github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/containerinsight"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscontainerinsightreceiver/internal/stores"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscontainerinsightreceiver/internal/stores/kubeletutil"
)

const (
	defaultCollectionInterval = 60 * time.Second
)

type Scraper struct {
	collectionInterval time.Duration
	cancel             context.CancelFunc

	kubeletClient *kubeletutil.KubeletClient
	hostInfo      hostInfoProvider
	store         *kubeletStore
	logger        *zap.Logger
}

type hostInfoProvider interface {
	GetClusterName() string
}

type kubeletStore struct {
	timestamp time.Time
	summary   *stats.Summary
}

func NewKubeletScraper(kubeletClient *kubeletutil.KubeletClient, hostInfo hostInfoProvider, logger *zap.Logger) *Scraper {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Scraper{
		collectionInterval: defaultCollectionInterval,
		cancel:             cancel,
		kubeletClient:      kubeletClient,
		hostInfo:           hostInfo,
		store:              new(kubeletStore),
		logger:             logger,
	}

	go s.startScrape(ctx)

	return s
}

func (s *Scraper) Shutdown() {
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *Scraper) GetMetrics() []pmetric.Metrics {
	var result []pmetric.Metrics

	store := s.store
	if store == nil || store.summary == nil {
		return result
	}

	// Aggregate volume metrics by volume name to avoid double counting
	volumeAggregates := make(map[string]*volumeAggregate)

	// Process volume metrics from pods
	for _, pod := range store.summary.Pods {
		if pod.VolumeStats == nil {
			continue
		}

		for _, volume := range pod.VolumeStats {
			if volume.FsStats.CapacityBytes == nil && volume.FsStats.UsedBytes == nil && volume.FsStats.AvailableBytes == nil {
				continue
			}

			// Only include volumes that have a PVC reference (actual persistent volumes)
			// Skip volumes like kube-api-access, tmp, config, etc.
			if volume.PVCRef == nil {
				s.logger.Debug("Skipping volume without PVC reference",
					zap.String("volume_name", volume.Name),
					zap.String("pod_name", pod.PodRef.Name))
				continue
			}

			// Get or create aggregate for this volume
			agg, exists := volumeAggregates[volume.Name]
			if !exists {
				agg = &volumeAggregate{
					volumeName: volume.Name,
					usedBytes:  0,
				}
				volumeAggregates[volume.Name] = agg
			}

			// Sum used bytes across pods (this makes sense)
			if volume.FsStats.UsedBytes != nil {
				agg.usedBytes += *volume.FsStats.UsedBytes
			}

			// Take max for capacity and available (they should be the same for same volume)
			if volume.FsStats.CapacityBytes != nil {
				if agg.capacityBytes == nil || *volume.FsStats.CapacityBytes > *agg.capacityBytes {
					agg.capacityBytes = volume.FsStats.CapacityBytes
				}
			}
			if volume.FsStats.AvailableBytes != nil {
				if agg.availableBytes == nil || *volume.FsStats.AvailableBytes > *agg.availableBytes {
					agg.availableBytes = volume.FsStats.AvailableBytes
				}
			}
		}
	}

	// Create metrics from aggregated data
	for _, agg := range volumeAggregates {
		volumeMetric := stores.NewCIMetric(ci.TypePV, s.logger)

		// Add aggregated volume metrics
		if agg.capacityBytes != nil {
			metricName := ci.MetricName(ci.TypePV, "_capacity")
			volumeMetric.AddField(metricName, float64(*agg.capacityBytes))
		}
		if agg.usedBytes > 0 {
			metricName := ci.MetricName(ci.TypePV, "_used")
			volumeMetric.AddField(metricName, float64(agg.usedBytes))
		}
		if agg.availableBytes != nil {
			metricName := ci.MetricName(ci.TypePV, "_available")
			volumeMetric.AddField(metricName, float64(*agg.availableBytes))
		}
		if agg.capacityBytes != nil && agg.usedBytes > 0 {
			metricName := ci.MetricName(ci.TypePV, "_utilization")
			volumeMetric.AddField(metricName, float64(agg.usedBytes)/float64(*agg.capacityBytes))
		}

		// Add tags
		volumeMetric.AddTag(ci.VolumeName, agg.volumeName)
		volumeMetric.AddTag(ci.MetricType, ci.TypePV)
		volumeMetric.AddTag(ci.ClusterNameKey, s.hostInfo.GetClusterName())

		// Only add metrics if we have fields
		if len(volumeMetric.GetFields()) > 0 {
			result = append(result, ci.ConvertToOTLPMetrics(volumeMetric.GetFields(), volumeMetric.GetTags(), s.logger))
		}
	}

	return result
}

type volumeAggregate struct {
	volumeName     string
	usedBytes      uint64
	capacityBytes  *uint64
	availableBytes *uint64
}

func (s *Scraper) startScrape(ctx context.Context) {
	ticker := time.NewTicker(s.collectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := s.scrape()
			if err != nil {
				s.logger.Warn("Failed to scrape kubelet metrics", zap.Error(err))
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *Scraper) scrape() error {
	timestamp := time.Now()

	summary, err := s.kubeletClient.Summary(s.logger)
	if err != nil {
		return err
	}

	s.store = &kubeletStore{
		timestamp: timestamp,
		summary:   summary,
	}

	return nil
}
