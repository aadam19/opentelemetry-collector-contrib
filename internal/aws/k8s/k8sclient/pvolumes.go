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

type PVolumeClient interface {
	GetVolumeCount() int
}

type noOpPVolumeClient struct{}

func (p *noOpPVolumeClient) GetVolumeCount() int {
	return 0
}

func (p *noOpPVolumeClient) shutdown() {
}

type PVolumeClientOption func(*PVolume)

func PVolumeSyncCheckerOption(checker initialSyncChecker) PVolumeClientOption {
	return func(p *PVolume) {
		p.syncChecker = checker
	}
}

type PVolume struct {
	stopChan chan struct{}
	stopped  bool

	store *ObjStore

	syncChecker initialSyncChecker

	mu         sync.RWMutex
	totalCount int
}

func (p *PVolume) refresh() {
	p.mu.Lock()
	defer p.mu.Unlock()
	totalCount := 0
	objsList := p.store.List()
	for _, obj := range objsList {
		_, ok := obj.(*corev1.PersistentVolume)
		if !ok {
			continue
		}
		totalCount++
	}
	p.totalCount = totalCount
}

func (p *PVolume) GetVolumeCount() int {
	if p.store.GetResetRefreshStatus() {
		p.refresh()
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.totalCount
}

func newPVolumeClient(clientSet kubernetes.Interface, logger *zap.Logger, options ...PVolumeClientOption) (*PVolume, error) {
	p := &PVolume{
		stopChan: make(chan struct{}),
	}

	for _, option := range options {
		option(p)
	}

	ctx := context.Background()
	if _, err := clientSet.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{}); err != nil {
		return nil, fmt.Errorf("cannot list PVCs. err: %w", err)
	}

	// Create a store to hold PVC objects
	p.store = NewObjStore(transformFuncPVolume, logger)
	// Create a ListWatch that knows how to list and watch PVCs
	lw := createPVolumeListWatch(clientSet)
	// Create a Reflector that watches PVCs and updates the store
	reflector := cache.NewReflector(lw, &corev1.PersistentVolume{}, p.store, 0)
	// Start the Reflector in a goroutine
	go reflector.Run(p.stopChan)

	if p.syncChecker != nil {
		// Check the init sync for potential connection issue
		p.syncChecker.Check(reflector, "PersistentVolume initial sync timeout")
	}

	return p, nil
}

func (p *PVolume) shutdown() {
	close(p.stopChan)
	p.stopped = true
}

func transformFuncPVolume(obj any) (any, error) {
	pvc, ok := obj.(*corev1.PersistentVolume)
	if !ok {
		return nil, fmt.Errorf("input obj %v is not PersistentVolume type", obj)
	}
	return pvc, nil
}

func createPVolumeListWatch(client kubernetes.Interface) cache.ListerWatcher {
	ctx := context.Background()
	return &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			return client.CoreV1().PersistentVolumes().List(ctx, opts)
		},
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			return client.CoreV1().PersistentVolumes().Watch(ctx, opts)
		},
	}
}
