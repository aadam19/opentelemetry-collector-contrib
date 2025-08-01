// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package k8sclient

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestIngressClient_IngressInfos(t *testing.T) {
	client := fake.NewSimpleClientset()
	logger := zap.NewNop()

	// Create test ingresses
	ingress1 := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-ingress-1",
			Namespace: "default",
			UID:       "uid-1",
			Labels:    map[string]string{"app": "test1"},
		},
		Spec: networkingv1.IngressSpec{
			Rules: []networkingv1.IngressRule{
				{Host: "test1.example.com"},
			},
		},
	}

	ingress2 := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-ingress-2",
			Namespace: "kube-system",
			UID:       "uid-2",
			Labels:    map[string]string{"app": "test2"},
		},
		Spec: networkingv1.IngressSpec{
			Rules: []networkingv1.IngressRule{
				{Host: "test2.example.com"},
			},
		},
	}

	_, err := client.NetworkingV1().Ingresses("default").Create(context.TODO(), ingress1, metav1.CreateOptions{})
	require.NoError(t, err)

	_, err = client.NetworkingV1().Ingresses("kube-system").Create(context.TODO(), ingress2, metav1.CreateOptions{})
	require.NoError(t, err)

	ingressClient, err := newIngressClient(client, logger)
	require.NoError(t, err)
	defer ingressClient.shutdown()

	// Wait for sync
	time.Sleep(100 * time.Millisecond)

	infos := ingressClient.IngressInfos()
	assert.Len(t, infos, 2)

	// Verify ingress info content
	infoMap := make(map[string]*IngressInfo)
	for _, info := range infos {
		infoMap[info.Name] = info
	}

	assert.Contains(t, infoMap, "test-ingress-1")
	assert.Contains(t, infoMap, "test-ingress-2")

	info1 := infoMap["test-ingress-1"]
	assert.Equal(t, "default", info1.Namespace)
	assert.Equal(t, "uid-1", info1.UID)
	assert.Equal(t, map[string]string{"app": "test1"}, info1.Labels)
	assert.Equal(t, "test1.example.com", info1.Spec.Rules[0].Host)

	info2 := infoMap["test-ingress-2"]
	assert.Equal(t, "kube-system", info2.Namespace)
	assert.Equal(t, "uid-2", info2.UID)
	assert.Equal(t, map[string]string{"app": "test2"}, info2.Labels)
	assert.Equal(t, "test2.example.com", info2.Spec.Rules[0].Host)
}

func TestIngressClient_GetNamespaceCount(t *testing.T) {
	// Create ingresses in different namespaces with unique UIDs
	ingresses := []*networkingv1.Ingress{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "ing1",
				Namespace: "default",
				UID:       "uid-1",
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "ing2",
				Namespace: "default",
				UID:       "uid-2",
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "ing3",
				Namespace: "kube-system",
				UID:       "uid-3",
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "ing4",
				Namespace: "test-ns",
				UID:       "uid-4",
			},
		},
	}

	// Convert to runtime.Object slice for fake client
	objects := make([]runtime.Object, len(ingresses))
	for i, ing := range ingresses {
		objects[i] = ing
	}

	client := fake.NewSimpleClientset(objects...)
	logger := zap.NewNop()

	ingressClient, err := newIngressClient(client, logger, ingressSyncCheckerOption(&mockSyncChecker{}))
	require.NoError(t, err)
	defer ingressClient.shutdown()

	// Directly populate the store to bypass reflector sync issues
	ingressObjects := make([]any, len(ingresses))
	for i, ing := range ingresses {
		ingressObjects[i] = ing
	}
	require.NoError(t, ingressClient.store.Replace(ingressObjects, ""))

	counts := ingressClient.GetNamespaceCount()
	expected := map[string]int{
		"default":     2,
		"kube-system": 1,
		"test-ns":     1,
	}

	assert.Equal(t, expected, counts)
}

func TestIngressClient_GetIngressCount(t *testing.T) {
	// Create multiple ingresses with unique UIDs
	ingresses := make([]*networkingv1.Ingress, 5)
	for i := 0; i < 5; i++ {
		ingresses[i] = &networkingv1.Ingress{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-ingress-" + string(rune('1'+i)),
				Namespace: "default",
				UID:       types.UID("uid-" + string(rune('1'+i))),
			},
		}
	}

	// Convert to runtime.Object slice for fake client
	objects := make([]runtime.Object, len(ingresses))
	for i, ing := range ingresses {
		objects[i] = ing
	}

	client := fake.NewSimpleClientset(objects...)
	logger := zap.NewNop()

	ingressClient, err := newIngressClient(client, logger, ingressSyncCheckerOption(&mockSyncChecker{}))
	require.NoError(t, err)
	defer ingressClient.shutdown()

	// Directly populate the store to bypass reflector sync issues
	ingressObjects := make([]any, len(ingresses))
	for i, ing := range ingresses {
		ingressObjects[i] = ing
	}
	require.NoError(t, ingressClient.store.Replace(ingressObjects, ""))

	count := ingressClient.GetIngressCount()
	assert.Equal(t, 5, count)
}

func TestIngressClient_EmptyCluster(t *testing.T) {
	client := fake.NewSimpleClientset()
	logger := zap.NewNop()

	ingressClient, err := newIngressClient(client, logger)
	require.NoError(t, err)
	defer ingressClient.shutdown()

	// Wait for sync
	time.Sleep(100 * time.Millisecond)

	infos := ingressClient.IngressInfos()
	assert.Empty(t, infos)

	counts := ingressClient.GetNamespaceCount()
	assert.Empty(t, counts)

	count := ingressClient.GetIngressCount()
	assert.Equal(t, 0, count)
}

func TestNoOpIngressClient(t *testing.T) {
	client := &noOpIngressClient{}

	infos := client.IngressInfos()
	assert.Empty(t, infos)

	counts := client.GetNamespaceCount()
	assert.Empty(t, counts)

	count := client.GetIngressCount()
	assert.Equal(t, 0, count)

	// Should not panic
	client.shutdown()
}

func TestTransformFuncIngress(t *testing.T) {
	ingress := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-ingress",
			Namespace: "default",
			UID:       "test-uid",
			Labels:    map[string]string{"app": "test"},
		},
		Spec: networkingv1.IngressSpec{
			Rules: []networkingv1.IngressRule{
				{Host: "test.example.com"},
			},
		},
		Status: networkingv1.IngressStatus{
			LoadBalancer: networkingv1.IngressLoadBalancerStatus{
				Ingress: []networkingv1.IngressLoadBalancerIngress{
					{IP: "192.168.1.1"},
				},
			},
		},
	}

	result, err := transformFuncIngress(ingress)
	require.NoError(t, err)

	info, ok := result.(*IngressInfo)
	require.True(t, ok)

	assert.Equal(t, "test-ingress", info.Name)
	assert.Equal(t, "default", info.Namespace)
	assert.Equal(t, "test-uid", info.UID)
	assert.Equal(t, map[string]string{"app": "test"}, info.Labels)
	assert.Equal(t, "test.example.com", info.Spec.Rules[0].Host)
	assert.Equal(t, "192.168.1.1", info.Status.LoadBalancer.Ingress[0].IP)
}

func TestTransformFuncIngress_InvalidType(t *testing.T) {
	_, err := transformFuncIngress("invalid")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "is not Ingress type")
}

func TestCreateIngressListWatch(t *testing.T) {
	client := fake.NewSimpleClientset()
	lw := createIngressListWatch(client, "default")

	// Test List
	listResult, err := lw.List(metav1.ListOptions{})
	require.NoError(t, err)

	ingressList, ok := listResult.(*networkingv1.IngressList)
	require.True(t, ok)
	assert.Empty(t, ingressList.Items)

	// Test Watch
	watcher, err := lw.Watch(metav1.ListOptions{})
	require.NoError(t, err)
	assert.NotNil(t, watcher)
	watcher.Stop()
}

func TestIngressClient_Shutdown(t *testing.T) {
	client := fake.NewSimpleClientset()
	logger := zap.NewNop()

	ingressClient, err := newIngressClient(client, logger)
	require.NoError(t, err)

	assert.False(t, ingressClient.stopped)

	ingressClient.shutdown()
	assert.True(t, ingressClient.stopped)

	// Channel should be closed
	select {
	case <-ingressClient.stopChan:
		// Expected - channel is closed
	default:
		t.Error("stopChan should be closed after shutdown")
	}
}

func TestIngressClient_WithSyncChecker(t *testing.T) {
	client := fake.NewSimpleClientset()
	logger := zap.NewNop()

	mockChecker := &mockSyncChecker{}
	ingressClient, err := newIngressClient(client, logger, ingressSyncCheckerOption(mockChecker))
	require.NoError(t, err)
	defer ingressClient.shutdown()

	assert.True(t, mockChecker.checkCalled)
}

// Mock sync checker for testing
type mockSyncChecker struct {
	checkCalled bool
}

func (m *mockSyncChecker) Check(reflector cacheReflector, msg string) {
	m.checkCalled = true
}
