// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package k8sclient // import "github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/k8s/k8sclient"

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

type IngressClient interface {
	IngressInfos() []*IngressInfo
	GetNamespaceCount() map[string]int
	GetIngressCount() int
}

type noOpIngressClient struct{}

func (n *noOpIngressClient) IngressInfos() []*IngressInfo {
	return []*IngressInfo{}
}

func (n *noOpIngressClient) GetNamespaceCount() map[string]int {
	return make(map[string]int)
}

func (n *noOpIngressClient) GetIngressCount() int {
	return 0
}

func (n *noOpIngressClient) shutdown() {
}

type ingressClientOption func(*ingressClient)

func ingressSyncCheckerOption(checker initialSyncChecker) ingressClientOption {
	return func(c *ingressClient) {
		c.syncChecker = checker
	}
}

type ingressClient struct {
	stopChan chan struct{}
	stopped  bool

	store *ObjStore

	syncChecker initialSyncChecker

	mu           sync.RWMutex
	ingressInfos []*IngressInfo
}

func (d *ingressClient) refresh() {
	d.mu.Lock()
	defer d.mu.Unlock()

	var ingressInfos []*IngressInfo
	objsList := d.store.List()
	for _, obj := range objsList {
		igInfo, ok := obj.(*IngressInfo)
		if !ok {
			continue
		}
		ingressInfos = append(ingressInfos, igInfo)
	}

	d.ingressInfos = ingressInfos
}

func (d *ingressClient) IngressInfos() []*IngressInfo {
	if d.store.GetResetRefreshStatus() {
		d.refresh()
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.ingressInfos
}

func (p *ingressClient) GetNamespaceCount() map[string]int {
	if p.store.GetResetRefreshStatus() {
		p.refresh()
	}
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make(map[string]int)
	for _, igInfo := range p.ingressInfos {
		result[igInfo.Namespace]++
	}
	return result
}

func (p *ingressClient) GetIngressCount() int {
	if p.store.GetResetRefreshStatus() {
		p.refresh()
	}
	p.mu.RLock()
	defer p.mu.RUnlock()

	return len(p.ingressInfos)
}

func (d *ingressClient) shutdown() {
	close(d.stopChan)
	d.stopped = true
}

func newIngressClient(clientSet kubernetes.Interface, logger *zap.Logger, options ...ingressClientOption) (*ingressClient, error) {
	d := &ingressClient{
		stopChan: make(chan struct{}),
	}

	for _, option := range options {
		option(d)
	}

	ctx := context.Background()
	if _, err := clientSet.NetworkingV1().Ingresses(metav1.NamespaceAll).List(ctx, metav1.ListOptions{}); err != nil {
		return nil, fmt.Errorf("failed to list ingress. err: %w", err)
	}

	d.store = NewObjStore(transformFuncIngress, logger)
	lw := createIngressListWatch(clientSet, metav1.NamespaceAll)
	reflector := cache.NewReflector(lw, &networkingv1.Ingress{}, d.store, 0)

	go reflector.Run(d.stopChan)

	if d.syncChecker != nil {
		// check the init sync for potential connection issue
		d.syncChecker.Check(reflector, "Ingress initial sync timeout")
	}

	return d, nil
}

func transformFuncIngress(obj any) (any, error) {
	ingress, ok := obj.(*networkingv1.Ingress)
	if !ok {
		return nil, fmt.Errorf("input obj %v is not Ingress type", obj)
	}
	info := new(IngressInfo)
	info.Name = ingress.Name
	info.Namespace = ingress.Namespace
	info.UID = string(ingress.UID)
	info.Labels = ingress.Labels
	info.Spec = ingress.Spec
	info.Status = ingress.Status
	return info, nil
}

func createIngressListWatch(client kubernetes.Interface, ns string) cache.ListerWatcher {
	ctx := context.Background()
	return &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			return client.NetworkingV1().Ingresses(ns).List(ctx, opts)
		},
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			return client.NetworkingV1().Ingresses(ns).Watch(ctx, opts)
		},
	}
}
