package k8sclient // import "github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/k8s/k8sclient"

import (
	networkingv1 "k8s.io/api/networking/v1"
)

type IngressInfo struct {
	Name      string
	Namespace string
	UID       string
	Labels    map[string]string
	Spec      networkingv1.IngressSpec
	Status    networkingv1.IngressStatus
}
