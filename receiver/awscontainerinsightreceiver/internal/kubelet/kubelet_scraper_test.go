// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kubelet

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	ci "github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/containerinsight"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscontainerinsightreceiver/internal/stores/kubeletutil"
)

const testClusterName = "test-cluster"

type mockHostInfoProvider struct{}

func (m *mockHostInfoProvider) GetClusterName() string {
	return testClusterName
}

// Mock kubelet client that implements the interface properly
type mockKubeletClientImpl struct {
	summary *stats.Summary
	err     error
}

func (m *mockKubeletClientImpl) Summary(_ *zap.Logger) (*stats.Summary, error) {
	return m.summary, m.err
}

func (m *mockKubeletClientImpl) ListPods() ([]corev1.Pod, error) {
	return nil, nil
}

func uint64Ptr(v uint64) *uint64 {
	return &v
}

func TestNewKubeletScraper(t *testing.T) {
	hostInfo := &mockHostInfoProvider{}
	// Create a real kubelet client for testing instantiation
	kubeletClient, err := kubeletutil.NewKubeletClient("127.0.0.1", "10250", nil, zap.NewNop())
	if err != nil {
		// If we can't create a real client, skip this test
		t.Skip("Cannot create kubelet client for testing")
	}

	scraper := NewKubeletScraper(kubeletClient, hostInfo, zap.NewNop())

	assert.NotNil(t, scraper)
	assert.Equal(t, defaultCollectionInterval, scraper.collectionInterval)
	assert.NotNil(t, scraper.cancel)
	assert.Equal(t, hostInfo, scraper.hostInfo)
	assert.NotNil(t, scraper.store)

	scraper.Shutdown()
}

func TestGetMetrics_WithPVCVolumes(t *testing.T) {
	hostInfo := &mockHostInfoProvider{}
	kubeletClient, err := kubeletutil.NewKubeletClient("127.0.0.1", "10250", nil, zap.NewNop())
	if err != nil {
		t.Skip("Cannot create kubelet client for testing")
	}

	scraper := NewKubeletScraper(kubeletClient, hostInfo, zap.NewNop())
	defer scraper.Shutdown()

	// Create test summary with PVC volumes
	summary := &stats.Summary{
		Pods: []stats.PodStats{
			{
				PodRef: stats.PodReference{Name: "pod1", Namespace: "default"},
				VolumeStats: []stats.VolumeStats{
					{
						Name: "pvc-volume-1",
						FsStats: stats.FsStats{
							CapacityBytes:  uint64Ptr(1000000000), // 1GB
							UsedBytes:      uint64Ptr(500000000),  // 500MB
							AvailableBytes: uint64Ptr(500000000),  // 500MB
						},
						PVCRef: &stats.PVCReference{Name: "test-pvc", Namespace: "default"},
					},
					{
						Name: "kube-api-access", // No PVCRef - should be skipped
						FsStats: stats.FsStats{
							CapacityBytes: uint64Ptr(1024),
							UsedBytes:     uint64Ptr(512),
						},
					},
				},
			},
		},
	}

	scraper.store = &kubeletStore{timestamp: time.Now(), summary: summary}

	metrics := scraper.GetMetrics()
	assert.Len(t, metrics, 1) // Only PVC volume should generate metrics

	// Verify metric content
	metric := metrics[0]
	rm := metric.ResourceMetrics()
	require.Equal(t, 1, rm.Len())

	attrs := rm.At(0).Resource().Attributes()
	clusterName, exists := attrs.Get(ci.ClusterNameKey)
	assert.True(t, exists)
	assert.Equal(t, testClusterName, clusterName.Str())

	volumeName, exists := attrs.Get(ci.PersistentVolumeName)
	assert.True(t, exists)
	assert.Equal(t, "pvc-volume-1", volumeName.Str())
}

func TestGetMetrics_VolumeAggregation(t *testing.T) {
	hostInfo := &mockHostInfoProvider{}
	kubeletClient, err := kubeletutil.NewKubeletClient("127.0.0.1", "10250", nil, zap.NewNop())
	if err != nil {
		t.Skip("Cannot create kubelet client for testing")
	}

	scraper := NewKubeletScraper(kubeletClient, hostInfo, zap.NewNop())
	defer scraper.Shutdown()

	// Create test summary with same volume across multiple pods
	summary := &stats.Summary{
		Pods: []stats.PodStats{
			{
				PodRef: stats.PodReference{Name: "pod1", Namespace: "default"},
				VolumeStats: []stats.VolumeStats{
					{
						Name: "shared-volume",
						FsStats: stats.FsStats{
							CapacityBytes:  uint64Ptr(1000000000), // 1GB
							UsedBytes:      uint64Ptr(300000000),  // 300MB
							AvailableBytes: uint64Ptr(700000000),  // 700MB
						},
						PVCRef: &stats.PVCReference{Name: "shared-pvc", Namespace: "default"},
					},
				},
			},
			{
				PodRef: stats.PodReference{Name: "pod2", Namespace: "default"},
				VolumeStats: []stats.VolumeStats{
					{
						Name: "shared-volume", // Same volume name
						FsStats: stats.FsStats{
							CapacityBytes:  uint64Ptr(1000000000), // 1GB
							UsedBytes:      uint64Ptr(200000000),  // 200MB
							AvailableBytes: uint64Ptr(800000000),  // 800MB
						},
						PVCRef: &stats.PVCReference{Name: "shared-pvc", Namespace: "default"},
					},
				},
			},
		},
	}

	scraper.store = &kubeletStore{timestamp: time.Now(), summary: summary}

	metrics := scraper.GetMetrics()
	assert.Len(t, metrics, 1) // Should aggregate into single metric

	// Verify aggregation by checking metric values
	metric := metrics[0]
	rm := metric.ResourceMetrics()
	sm := rm.At(0).ScopeMetrics()

	var usedBytes, capacityBytes float64
	for i := 0; i < sm.Len(); i++ {
		scopeMetrics := sm.At(i)
		metrics := scopeMetrics.Metrics()
		for j := 0; j < metrics.Len(); j++ {
			m := metrics.At(j)
			dps := m.Gauge().DataPoints()
			if dps.Len() > 0 {
				switch m.Name() {
				case ci.PersistentVolumeUsed:
					usedBytes = dps.At(0).DoubleValue()
				case ci.PersistentVolumeCapacity:
					capacityBytes = dps.At(0).DoubleValue()
				}
			}
		}
	}

	// Used bytes should be sum: 300MB + 200MB = 500MB
	assert.Equal(t, float64(500000000), usedBytes)
	// Capacity should be max: 1GB
	assert.Equal(t, float64(1000000000), capacityBytes)
}

func TestGetMetrics_NoStore(t *testing.T) {
	hostInfo := &mockHostInfoProvider{}
	kubeletClient, err := kubeletutil.NewKubeletClient("127.0.0.1", "10250", nil, zap.NewNop())
	if err != nil {
		t.Skip("Cannot create kubelet client for testing")
	}

	scraper := NewKubeletScraper(kubeletClient, hostInfo, zap.NewNop())
	defer scraper.Shutdown()

	// Don't set store
	metrics := scraper.GetMetrics()
	assert.Empty(t, metrics)
}

func TestMockKubeletClient(t *testing.T) {
	// Test the mock kubelet client implementation
	summary := &stats.Summary{
		Pods: []stats.PodStats{
			{
				PodRef: stats.PodReference{Name: "test-pod", Namespace: "default"},
			},
		},
	}

	mockClient := &mockKubeletClientImpl{
		summary: summary,
		err:     nil,
	}

	// Test Summary method
	result, err := mockClient.Summary(zap.NewNop())
	assert.NoError(t, err)
	assert.Equal(t, summary, result)

	// Test ListPods method
	pods, err := mockClient.ListPods()
	assert.NoError(t, err)
	assert.Nil(t, pods)

	// Test error case
	mockClient.err = assert.AnError
	result, err = mockClient.Summary(zap.NewNop())
	assert.Error(t, err)
	assert.Equal(t, summary, result) // Still returns summary even with error
}
