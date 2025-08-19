// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package k8sclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

var pvcObjects = []runtime.Object{
	&corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-1",
			Namespace: "test-namespace",
			UID:       "pvc-1-uid",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimPending,
		},
	},
	&corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-2",
			Namespace: "test-namespace",
			UID:       "pvc-2-uid",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	},
	&corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-3",
			Namespace: "another-namespace",
			UID:       "pvc-3-uid",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimLost,
		},
	},
}

func TestPVolumeClaimClient_GetPVCMetrics(t *testing.T) {
	setOption := PVolumeClaimSyncCheckerOption(&mockReflectorSyncChecker{})

	fakeClientSet := fake.NewSimpleClientset(pvcObjects...)
	client, _ := newPVolumeClaimClient(fakeClientSet, zap.NewNop(), setOption)

	pvcs := make([]any, len(pvcObjects))
	for i := range pvcObjects {
		pvcs[i] = pvcObjects[i]
	}
	assert.NoError(t, client.store.Replace(pvcs, ""))

	metrics := client.GetPVCMetrics()

	// Test namespace
	expectedNamespaceCount := map[string]int{
		"test-namespace":    2,
		"another-namespace": 1,
	}
	expectedNamespacePending := map[string]int{
		"test-namespace": 1, // pvc-2 is pending
	}
	expectedNamespaceBound := map[string]int{
		"test-namespace": 1, // pvc-1 is bound
	}
	expectedNamespaceLost := map[string]int{
		"another-namespace": 1, // pvc-3 is lost
	}

	assert.Equal(t, expectedNamespaceCount, metrics.NamespaceCount)
	assert.Equal(t, expectedNamespacePending, metrics.NamespacePending)
	assert.Equal(t, expectedNamespaceBound, metrics.NamespaceBound)
	assert.Equal(t, expectedNamespaceLost, metrics.NamespaceLost)

	// Test cluster-level metrics
	assert.Equal(t, 3, metrics.ClusterCount)   // total PVCs
	assert.Equal(t, 1, metrics.ClusterPending) // 1 pending
	assert.Equal(t, 1, metrics.ClusterBound)   // 1 bound
	assert.Equal(t, 1, metrics.ClusterLost)    // 1 lost

	// Test individual PVC phases
	expectedPVCPhases := map[string]corev1.PersistentVolumeClaimPhase{
		"test-namespace/pvc-1":    corev1.ClaimPending,
		"test-namespace/pvc-2":    corev1.ClaimBound,
		"another-namespace/pvc-3": corev1.ClaimLost,
	}
	assert.Equal(t, expectedPVCPhases, metrics.PVCPhases)

	client.shutdown()
	assert.True(t, client.stopped)
}

func TestTransformFuncPVolumeClaim(t *testing.T) {
	info, err := transformFuncPVolumeClaim(nil)
	assert.Nil(t, info)
	assert.Error(t, err)

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "test-namespace",
		},
	}
	result, err := transformFuncPVolumeClaim(pvc)
	assert.NoError(t, err)
	assert.Equal(t, pvc, result)
}

func TestNoOpPVolumeClaimClient(t *testing.T) {
	client := &noOpPVolumeClaimClient{}

	// Test GetPVCMetrics returns empty but valid metrics
	metrics := client.GetPVCMetrics()
	assert.NotNil(t, metrics)
	assert.Equal(t, map[string]int{}, metrics.NamespaceCount)
	assert.Equal(t, map[string]int{}, metrics.NamespacePending)
	assert.Equal(t, map[string]int{}, metrics.NamespaceBound)
	assert.Equal(t, map[string]int{}, metrics.NamespaceLost)
	assert.Equal(t, 0, metrics.ClusterCount)
	assert.Equal(t, 0, metrics.ClusterPending)
	assert.Equal(t, 0, metrics.ClusterBound)
	assert.Equal(t, 0, metrics.ClusterLost)
	assert.Equal(t, map[string]corev1.PersistentVolumeClaimPhase{}, metrics.PVCPhases)

	// Should not panic
	client.shutdown()
}
