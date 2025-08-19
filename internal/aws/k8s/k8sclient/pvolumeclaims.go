// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package k8sclient // import "github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/k8s/k8sclient"

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// PVCMetrics holds all the metrics for PVCs
type PVCMetrics struct {
	// Namespace-level metrics
	NamespaceCount   map[string]int
	NamespacePending map[string]int
	NamespaceBound   map[string]int
	NamespaceLost    map[string]int

	// Cluster-level metrics
	ClusterCount   int
	ClusterPending int
	ClusterBound   int
	ClusterLost    int

	// Individual PVC metrics
	PVCPhases map[string]corev1.PersistentVolumeClaimPhase
}

type PVolumeClaimClient interface {
	GetPVCMetrics() *PVCMetrics
}

type noOpPVolumeClaimClient struct{}

func (p *noOpPVolumeClaimClient) GetPVCMetrics() *PVCMetrics {
	return &PVCMetrics{
		NamespaceCount:   make(map[string]int),
		NamespacePending: make(map[string]int),
		NamespaceBound:   make(map[string]int),
		NamespaceLost:    make(map[string]int),
		PVCPhases:        make(map[string]corev1.PersistentVolumeClaimPhase),
	}
}

func (p *noOpPVolumeClaimClient) shutdown() {
}

type PVolumeClaimClientOption func(*PVolumeClaim)

func PVolumeClaimSyncCheckerOption(checker initialSyncChecker) PVolumeClaimClientOption {
	return func(p *PVolumeClaim) {
		p.syncChecker = checker
	}
}

type PVolumeClaim struct {
	stopChan chan struct{}
	stopped  bool

	store *ObjStore

	syncChecker initialSyncChecker

	mu      sync.RWMutex
	metrics *PVCMetrics
}

func (p *PVolumeClaim) refresh() {
	p.mu.Lock()
	defer p.mu.Unlock()

	metrics := &PVCMetrics{
		NamespaceCount:   make(map[string]int),
		NamespacePending: make(map[string]int),
		NamespaceBound:   make(map[string]int),
		NamespaceLost:    make(map[string]int),
		PVCPhases:        make(map[string]corev1.PersistentVolumeClaimPhase),
	}

	objsList := p.store.List()
	for _, obj := range objsList {
		pvc, ok := obj.(*corev1.PersistentVolumeClaim)
		if !ok {
			continue
		}

		namespace := pvc.Namespace
		pvcKey := fmt.Sprintf("%s/%s", namespace, pvc.Name)
		phase := pvc.Status.Phase

		// Increment counters
		metrics.NamespaceCount[namespace]++
		metrics.ClusterCount++
		metrics.PVCPhases[pvcKey] = phase

		// Phase specific counters
		switch phase {
		case corev1.ClaimPending:
			metrics.NamespacePending[namespace]++
			metrics.ClusterPending++
		case corev1.ClaimBound:
			metrics.NamespaceBound[namespace]++
			metrics.ClusterBound++
		case corev1.ClaimLost:
			metrics.NamespaceLost[namespace]++
			metrics.ClusterLost++
		default:
			continue
		}
	}
	p.metrics = metrics
}

func (p *PVolumeClaim) GetPVCMetrics() *PVCMetrics {
	if p.store.GetResetRefreshStatus() {
		p.refresh()
	}
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.metrics
}

func newPVolumeClaimClient(clientSet kubernetes.Interface, logger *zap.Logger, options ...PVolumeClaimClientOption) (*PVolumeClaim, error) {
	p := &PVolumeClaim{
		stopChan: make(chan struct{}),
		metrics: &PVCMetrics{
			NamespaceCount:   make(map[string]int),
			NamespacePending: make(map[string]int),
			NamespaceBound:   make(map[string]int),
			NamespaceLost:    make(map[string]int),
			PVCPhases:        make(map[string]corev1.PersistentVolumeClaimPhase),
		},
	}

	for _, option := range options {
		option(p)
	}

	ctx := context.Background()
	if _, err := clientSet.CoreV1().PersistentVolumeClaims(metav1.NamespaceAll).List(ctx, metav1.ListOptions{}); err != nil {
		return nil, fmt.Errorf("cannot list PVCs. err: %w", err)
	}

	// Create a store to hold PVC objects
	p.store = NewObjStore(transformFuncPVolumeClaim, logger)
	// Create a ListWatch that knows how to list and watch PVCs
	lw := createPVolumeClaimListWatch(clientSet, metav1.NamespaceAll)
	// Create a Reflector that watches PVCs and updates the store
	reflector := cache.NewReflector(lw, &corev1.PersistentVolumeClaim{}, p.store, 0)
	// Start the Reflector in a goroutine
	go reflector.Run(p.stopChan)

	if p.syncChecker != nil {
		// Check the init sync for potential connection issue
		p.syncChecker.Check(reflector, "PersistentVolumeClaim initial sync timeout")
	}

	return p, nil
}

func (p *PVolumeClaim) shutdown() {
	close(p.stopChan)
	p.stopped = true
}

func transformFuncPVolumeClaim(obj any) (any, error) {
	pvc, ok := obj.(*corev1.PersistentVolumeClaim)
	if !ok {
		return nil, fmt.Errorf("input obj %v is not PersistentVolumeClaim type", obj)
	}
	return pvc, nil
}

func createPVolumeClaimListWatch(client kubernetes.Interface, ns string) cache.ListerWatcher {
	ctx := context.Background()
	return &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			return client.CoreV1().PersistentVolumeClaims(ns).List(ctx, opts)
		},
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			return client.CoreV1().PersistentVolumeClaims(ns).Watch(ctx, opts)
		},
	}
}
