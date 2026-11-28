/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	nvcav1new "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v1"
	nvcav2beta1 "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v2beta1"
	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	utilruntime.Must(nvcav2beta1.AddToScheme(s))
	utilruntime.Must(nvcav1new.AddToScheme(s))
	utilruntime.Must(corev1.AddToScheme(s))
	utilruntime.Must(appsv1.AddToScheme(s))
	utilruntime.Must(coordv1.AddToScheme(s))
	utilruntime.Must(storagev1.AddToScheme(s))
	utilruntime.Must(batchv1.AddToScheme(s))
	return s
}

func newTestRESTMapper(s *runtime.Scheme) meta.RESTMapper {
	gvs := s.PreferredVersionAllGroups()
	rm := meta.NewDefaultRESTMapper(gvs)
	for gvk := range s.AllKnownTypes() {
		scope := meta.RESTScopeNamespace
		switch gvk.Kind {
		case "PersistentVolume", "StorageClass":
			scope = meta.RESTScopeRoot
		}
		rm.Add(gvk, scope)
	}
	return rm
}

func newFakeClient(s *runtime.Scheme, objs ...client.Object) client.Client {
	return clientfake.NewClientBuilder().
		WithScheme(s).
		WithRESTMapper(newTestRESTMapper(s)).
		WithObjects(objs...).
		WithStatusSubresource(&nvcav1new.StorageRequest{}).
		Build()
}

func newBool(v bool) *bool       { return &v }
func newInt32(v int32) *int32    { return &v }
func newString(v string) *string { return &v }

func mergeMaps(m1, m2 map[string]string) map[string]string {
	merged := make(map[string]string)
	for k, v := range m1 {
		merged[k] = v
	}
	for key, value := range m2 {
		merged[key] = value
	}
	return merged
}

func TestHelperAPIs(t *testing.T) {
	r := &Reconciler{ICMSRequestNamespace: nvcatypes.DefaultICMSRequestNamespace}

	stReq := nvcav1new.StorageRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stor-req",
			Namespace: nvcatypes.DefaultICMSRequestNamespace,
			Labels: map[string]string{
				"function-version-id": "random-id",
			},
		},
		Spec: nvcav1new.StorageRequestSpec{
			ICMSRequestName: "random-req",
			Type:            nvcav1new.SharedStorageRequest,
			SharedStorage: &nvcav1new.SharedStorageSpec{
				SMBContainerImage:    "smb:latest",
				WorkerPullSecretName: "foo-worker",
			},
		},
	}
	err := r.validateStorageRequest(&stReq, nil)
	assert.Nil(t, err)

	stReq = nvcav1new.StorageRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stor-req",
			Namespace: nvcatypes.DefaultICMSRequestNamespace,
			Labels: map[string]string{
				"function-version-id": "random-id",
			},
		},
		Spec: nvcav1new.StorageRequestSpec{
			ICMSRequestName: "random-req",
			Type:            nvcav1new.InternalPersistentStorageRequest,
			InternalPersistentStorage: &nvcav1new.InternalPersistentStorageSpec{
				StorageClassName: "random",
			},
		},
	}
	err = r.validateStorageRequest(&stReq, nil)
	assert.Nil(t, err)

	// Instance namespace fallback: request name empty but namespace = instance ns name
	stReq = nvcav1new.StorageRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "shared-storage",
			Namespace: "sr-275d0ad9-f998-4abc-8bb0-75772343854b",
			Labels: map[string]string{
				"function-version-id": "random-id",
			},
		},
		Spec: nvcav1new.StorageRequestSpec{
			Type: nvcav1new.SharedStorageRequest,
			SharedStorage: &nvcav1new.SharedStorageSpec{
				SMBContainerImage:    "smb:latest",
				WorkerPullSecretName: "foo-worker",
			},
		},
	}
	err = r.validateStorageRequest(&stReq, nil)
	assert.Nil(t, err)

	stReq = nvcav1new.StorageRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stor-req",
			Namespace: nvcatypes.DefaultICMSRequestNamespace,
			Labels: map[string]string{
				"function-version-id": "random-id",
			},
		},
		Spec: nvcav1new.StorageRequestSpec{
			ICMSRequestName: "random-req",
			Type:            nvcav1new.ModelCacheRequest,
			ModelCache: &nvcav1new.ModelCacheSpec{
				Encryption: &nvcav1new.ModelCacheEncryption{
					Required: true,
				},
			},
			InternalPersistentStorage: &nvcav1new.InternalPersistentStorageSpec{
				StorageClassName: "random",
			},
		},
	}
	err = r.validateStorageRequest(&stReq, nil)
	assert.NotNil(t, err)

	stReq = nvcav1new.StorageRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stor-req",
			Namespace: nvcatypes.DefaultICMSRequestNamespace,
			Labels: map[string]string{
				"function-version-id": "random-id",
			},
		},
		Spec: nvcav1new.StorageRequestSpec{
			Type: nvcav1new.ModelCacheRequest,
			ModelCache: &nvcav1new.ModelCacheSpec{
				Encryption: &nvcav1new.ModelCacheEncryption{
					Required: true,
				},
			},
		},
	}
	err = r.validateStorageRequest(&stReq, nil)
	assert.NotNil(t, err)

	stReq = nvcav1new.StorageRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stor-req",
			Namespace: nvcatypes.DefaultICMSRequestNamespace,
			Labels: map[string]string{
				"function-version-id": "random-id",
			},
		},
		Spec: nvcav1new.StorageRequestSpec{
			Type: nvcav1new.ModelCacheRequest,
		},
	}
	err = r.validateStorageRequest(&stReq, nil)
	assert.NotNil(t, err)

	stReq = nvcav1new.StorageRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stor-req",
			Namespace: nvcatypes.DefaultICMSRequestNamespace,
			Labels: map[string]string{
				"function-version-id": "random-id",
			},
		},
		Spec: nvcav1new.StorageRequestSpec{
			Type: nvcav1new.InternalPersistentStorageRequest,
		},
	}
	err = r.validateStorageRequest(&stReq, nil)
	assert.NotNil(t, err)

	stReq = nvcav1new.StorageRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stor-req",
			Namespace: nvcatypes.DefaultICMSRequestNamespace,
			Labels: map[string]string{
				"function-version-id": "random-id",
			},
		},
		Spec: nvcav1new.StorageRequestSpec{
			Type: nvcav1new.SharedStorageRequest,
		},
	}
	err = r.validateStorageRequest(&stReq, nil)
	assert.NotNil(t, err)
}

// namespaceEnforcingClient makes the fake client behave like a real API server
// on the clusters in nvbugs#7055457: creating a namespaced object whose
// namespace does not exist is rejected with
// `namespaces "nvca-modelcache-init" not found`. The fake client otherwise
// happily creates objects in namespaces that were never created.
type namespaceEnforcingClient struct {
	client.Client
}

func (c namespaceEnforcingClient) Create(ctx context.Context,
	obj client.Object, opts ...client.CreateOption,
) error {
	if ns := obj.GetNamespace(); ns != "" {
		err := c.Client.Get(ctx, client.ObjectKey{Name: ns}, &corev1.Namespace{})
		if apierrors.IsNotFound(err) {
			return apierrors.NewNotFound(corev1.Resource("namespaces"), ns)
		}
		if err != nil {
			return err
		}
	}
	return c.Client.Create(ctx, obj, opts...)
}

// TestEnsureCacheMountOptionsConfigMapCreatesInitNamespace reproduces
// nvbugs#7055457 on the model-cache start-up path: on a cluster where nothing
// has created nvca-modelcache-init, every object NVCA puts in that namespace
// fails with `namespaces "nvca-modelcache-init" not found`. The agent must
// provision the namespace it hard-codes rather than assume the platform did.
func TestEnsureCacheMountOptionsConfigMapCreatesInitNamespace(t *testing.T) {
	ctx := context.Background()
	s := newTestScheme()
	c := namespaceEnforcingClient{Client: newFakeClient(s)}

	// The namespace does not exist, exactly as on gcp-usc1-ct1 /
	// forge-az31-ct1 / aws-use1-dev1 in the report.
	nsErr := c.Get(ctx, client.ObjectKey{Name: ModelCacheInitNamespace}, &corev1.Namespace{})
	assert.True(t, apierrors.IsNotFound(nsErr), "test setup: %s must be absent", ModelCacheInitNamespace)

	err := EnsureCacheMountOptionsConfigMap(ctx, c, DefaultCacheMountOptionsConfigMapName)
	assert.NoError(t, err,
		"model cache start-up must create namespace %s before seeding its mount option ConfigMap",
		ModelCacheInitNamespace)

	cm := &corev1.ConfigMap{}
	assert.NoError(t, c.Get(ctx, client.ObjectKey{
		Namespace: ModelCacheInitNamespace,
		Name:      DefaultCacheMountOptionsConfigMapName,
	}, cm), "mount option ConfigMap must exist in %s after start-up", ModelCacheInitNamespace)

	ns := &corev1.Namespace{}
	if assert.NoError(t, c.Get(ctx, client.ObjectKey{Name: ModelCacheInitNamespace}, ns)) {
		assert.Equal(t, "nvca", ns.Labels["app.kubernetes.io/managed-by"])
	}
}
