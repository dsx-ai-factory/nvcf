// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/containerd"
)

// A /v1/restore that fails after the restored engine ran in the
// placeholder, with its NIXL KV connector not resumed, disposes of the
// placeholder as the automatic restores do: the caller is not relied on
// to delete a pod that reads Ready and serves nothing. A member of a group
// restore is left to its group's leader, and any other failure is
// answered as before.
func TestRestoreHandler_DisposesAPlaceholderLeftNotServing(t *testing.T) {
	notServing := func() error {
		return restoredNotServing(&containerd.ContainerInfo{Labels: map[string]string{
			"io.kubernetes.pod.namespace":  "sr-1",
			"io.kubernetes.pod.name":       "fn-0",
			"io.kubernetes.pod.uid":        "u1",
			"io.kubernetes.container.name": "inference",
		}}, errors.New("nixl-resume: exit status 1"))
	}
	for _, tc := range []struct {
		name     string
		session  string
		err      error
		disposed bool
	}{
		{"single pod, connector not resumed", "", notServing(), true},
		{"single pod, GPU fault on top", "", &gpuFaultError{cause: notServing()}, true},
		{"group member", "fabric-session-1", notServing(), false},
		{"any other failure", "", errors.New("criu restore: exit status 1"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := placeholderPod(ownedBy("ReplicaSet", true))
			kc := fake.NewSimpleClientset(pod)
			a := &Agent{kubeClient: kc, log: logrus.New()}
			prev := restoreForRequest
			restoreForRequest = func(*Agent, context.Context, RestoreRequest) (*RestoreResult, error) { return nil, tc.err }
			t.Cleanup(func() { restoreForRequest = prev })

			body, _ := json.Marshal(RestoreRequest{CheckpointID: "abc__20261008-101010", PlaceholderNamespace: pod.Namespace,
				PlaceholderPodName: pod.Name, GPUShareFabricSession: tc.session})
			w := httptest.NewRecorder()
			a.restoreHandler(w, httptest.NewRequest(http.MethodPost, "/v1/restore", strings.NewReader(string(body))))

			if w.Code != http.StatusInternalServerError {
				t.Errorf("status %d, want 500", w.Code)
			}
			// The disposal runs after the answer: wait for it, or long
			// enough to see that it does not come.
			disposed := false
			for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
				if _, gerr := kc.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{}); gerr != nil {
					disposed = true
					break
				}
				if !tc.disposed {
					time.Sleep(200 * time.Millisecond)
					break
				}
			}
			if disposed != tc.disposed {
				t.Errorf("placeholder disposed = %v, want %v", disposed, tc.disposed)
			}
		})
	}
}

// restoredNotServing names the placeholder from its container's labels,
// and the mark survives the wrapping on the way to the handler.
func TestRestoredNotServing_NamesThePlaceholder(t *testing.T) {
	cause := errors.New("nixl-resume: exit status 1")
	err := &gpuFaultError{cause: restoredNotServing(&containerd.ContainerInfo{Labels: map[string]string{
		"io.kubernetes.pod.namespace":  "sr-1",
		"io.kubernetes.pod.name":       "fn-0",
		"io.kubernetes.pod.uid":        "u1",
		"io.kubernetes.container.name": "inference",
	}}, cause)}
	var e *restoredNotServingError
	if !errors.As(err, &e) {
		t.Fatalf("%v: not marked", err)
	}
	if e.Namespace != "sr-1" || e.Pod != "fn-0" || e.UID != "u1" || e.Container != "inference" {
		t.Errorf("placeholder %+v", e)
	}
	if !errors.Is(err, cause) || e.Error() != cause.Error() {
		t.Errorf("cause lost: %v", err)
	}
}

// The answer does not wait for the disposal, which may retry for minutes
// (a cold start in place, a delete): the caller has its error at once.
func TestRestoreHandler_AnswersBeforeTheDisposal(t *testing.T) {
	pod := placeholderPod(ownedBy("ReplicaSet", true))
	kc := fake.NewSimpleClientset(pod)
	release := make(chan struct{})
	kc.PrependReactor("get", "pods", func(k8stesting.Action) (bool, kruntime.Object, error) {
		<-release // the API server is slow
		return false, nil, nil
	})
	t.Cleanup(func() { close(release) })
	a := &Agent{kubeClient: kc, log: logrus.New()}
	prev := restoreForRequest
	restoreForRequest = func(*Agent, context.Context, RestoreRequest) (*RestoreResult, error) {
		return nil, restoredNotServing(&containerd.ContainerInfo{Labels: map[string]string{
			"io.kubernetes.pod.namespace": "sr-1", "io.kubernetes.pod.name": "fn-0", "io.kubernetes.pod.uid": "u1",
			"io.kubernetes.container.name": "inference"}}, errors.New("nixl-resume: exit status 1"))
	}
	t.Cleanup(func() { restoreForRequest = prev })

	body, _ := json.Marshal(RestoreRequest{CheckpointID: "abc__20261008-101010", PlaceholderNamespace: pod.Namespace, PlaceholderPodName: pod.Name})
	done := make(chan int)
	go func() {
		w := httptest.NewRecorder()
		a.restoreHandler(w, httptest.NewRequest(http.MethodPost, "/v1/restore", strings.NewReader(string(body))))
		done <- w.Code
	}()
	select {
	case code := <-done:
		if code != http.StatusInternalServerError {
			t.Errorf("status %d, want 500", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the answer waits for the placeholder's disposal")
	}
}

// The /v1/restore placeholder's restore marker is removed only once the
// background disposal is done.
func TestRestoreHandler_ReleasesTheMarkerAfterTheDisposal(t *testing.T) {
	pod := placeholderPod(ownedBy("ReplicaSet", true))
	kc := fake.NewSimpleClientset(pod)
	a := &Agent{kubeClient: kc, log: logrus.New()}
	released := make(chan bool, 1)
	prev := restoreForRequest
	restoreForRequest = func(*Agent, context.Context, RestoreRequest) (*RestoreResult, error) {
		e := restoredNotServing(&containerd.ContainerInfo{Labels: map[string]string{
			"io.kubernetes.pod.namespace": "sr-1", "io.kubernetes.pod.name": "fn-0", "io.kubernetes.pod.uid": "u1",
			"io.kubernetes.container.name": "inference"}}, errors.New("nixl-resume: exit status 1")).(*restoredNotServingError)
		e.release = func() {
			_, gerr := kc.CoreV1().Pods("sr-1").Get(context.Background(), "fn-0", metav1.GetOptions{})
			released <- gerr != nil // the pod is gone by then
		}
		return nil, e
	}
	t.Cleanup(func() { restoreForRequest = prev })
	body, _ := json.Marshal(RestoreRequest{CheckpointID: "abc__20261008-101010", PlaceholderNamespace: pod.Namespace, PlaceholderPodName: pod.Name})
	a.restoreHandler(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/restore", strings.NewReader(string(body))))
	select {
	case afterDisposal := <-released:
		if !afterDisposal {
			t.Error("the marker was released before the placeholder was disposed of")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the marker was never released")
	}
}
