// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package k8sclient

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

type PVolumeClaimClient interface {
	GetNamespaceCount() map[string]int
	GetClaimCount() int
}

type noOpPVolumeClaimClient struct{}

func (p *noOpPVolumeClaimClient) GetNamespaceCount() map[string]int {
	return map[string]int{}
}

func (p *noOpPVolumeClaimClient) GetClaimCount() int {
	return 0
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

	mu         sync.RWMutex
	nsCount    map[string]int
	totalCount int
}

func (p *PVolumeClaim) refresh() {
	p.mu.Lock()
	defer p.mu.Unlock()

	nsCount := make(map[string]int)
	totalCount := 0

	objsList := p.store.List()
	for _, obj := range objsList {
		pvc, ok := obj.(*corev1.PersistentVolumeClaim)
		if !ok {
			continue
		}
		nsCount[pvc.Namespace]++
		totalCount++
	}

	p.nsCount = nsCount
	p.totalCount = totalCount
}

func (p *PVolumeClaim) GetNamespaceCount() map[string]int {
	if p.store.GetResetRefreshStatus() {
		p.refresh()
	}
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make(map[string]int)
	for ns, count := range p.nsCount {
		result[ns] = count
	}
	return result
}

func (p *PVolumeClaim) GetClaimCount() int {
	if p.store.GetResetRefreshStatus() {
		p.refresh()
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.totalCount
}

func newPVolumeClaimClient(clientSet kubernetes.Interface, logger *zap.Logger, options ...PVolumeClaimClientOption) (*PVolumeClaim, error) {
	p := &PVolumeClaim{
		stopChan: make(chan struct{}),
		nsCount:  make(map[string]int),
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

func transformFuncPVolumeClaim(obj interface{}) (interface{}, error) {
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
