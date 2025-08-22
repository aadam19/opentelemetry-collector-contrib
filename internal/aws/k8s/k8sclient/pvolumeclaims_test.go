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
	assert.Equal(t, map[string]corev1.PersistentVolumeClaimPhase{}, metrics.PVCPhases)

	// Should not panic
	client.shutdown()
}
