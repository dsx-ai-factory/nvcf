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

package nvca

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/core"
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	nvcametrics "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/metrics"
	mscontroller "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/miniservice"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance/readiness"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance/rollout"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v1alpha1"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

// utilsProvenanceEnforcing reports whether the operator set the provenance mode to enforce for this agent.
func utilsProvenanceEnforcing() bool {
	return strings.EqualFold(os.Getenv(types.UtilsProvenanceModeEnv), "enforce")
}

// newUtilsProvenanceChecker builds the readiness checker: the canary is sent as the canary service account through an
// impersonating client, and permissions are reviewed with SubjectAccessReviews so every binding counts.
func newUtilsProvenanceChecker(cfg *rest.Config, kube kubernetes.Interface, metrics *nvcametrics.Metrics,
	log logrus.FieldLogger) (*readiness.Checker, error) {
	icfg := rest.CopyConfig(cfg)
	icfg.Impersonate = rest.ImpersonationConfig{UserName: readiness.CanaryUsername()}
	impersonated, err := kubernetes.NewForConfig(icfg)
	if err != nil {
		return nil, err
	}
	return &readiness.Checker{
		Canary:   readiness.KubeCanary{Impersonated: impersonated},
		Accounts: readiness.KubeAccounts{Client: kube},
		Reviewer: readiness.KubeReviewer{Client: kube},
		Objects:  readiness.KubeObjects{Client: kube},
		Recorder: readiness.MetricsRecorder{Inc: metrics.IncUtilsProvenanceCheck, Set: metrics.SetUtilsProvenanceReadiness},
		Log:      log,
	}, nil
}

// inventoryScanInterval is how often the post-enforcement inventory is scanned while enforcement is verified.
const inventoryScanInterval = 2 * time.Minute

// addUtilsProvenanceReadiness registers the readiness check and the inventory scan with the manager when the provenance
// mode is enforce. In audit mode nothing runs, the readiness metrics stay at zero, and the returned gate is nil, which
// refuses: nothing is verified. The returned gate is what replicas and label discovery must pass: enforcement and the
// instance RBAC verified and fresh, and a clean inventory scan covering the current state.
func addUtilsProvenanceReadiness(ctx context.Context, mgr manager.Manager, kube kubernetes.Interface,
	metrics *nvcametrics.Metrics, systemNamespace string) (readiness.Checkable, error) {
	if !utilsProvenanceEnforcing() {
		return nil, nil
	}
	log := core.GetLogger(ctx)
	checker, err := newUtilsProvenanceChecker(mgr.GetConfig(), kube, metrics, log)
	if err != nil {
		return nil, err
	}
	tracker := &rollout.Tracker{Store: rollout.ConfigMapStore{Client: kube, Namespace: systemNamespace}}
	checker.OnStatus = func(s readiness.Status) {
		if err := tracker.Observe(ctx, s.EnforcementVerified); err != nil {
			log.WithError(err).Warn("Failed to record the utils provenance rollout state")
		}
	}
	gate := rollout.Gate{Readiness: readiness.Gate{Verifier: checker}, Tracker: tracker}

	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		log.Info("Starting the utils provenance readiness check")
		checker.Run(ctx)
		return nil
	})); err != nil {
		return nil, err
	}
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		runInventoryScans(ctx, log, tracker, checker, rollout.KubeLister{Client: kube, Trust: miniServiceTrust{reader: mgr.GetClient()}}, metrics)
		return nil
	})); err != nil {
		return nil, err
	}
	log.WithFields(logrus.Fields{"canary_user": readiness.CanaryUsername()}).Info("Registered the utils provenance readiness check")
	return gate, nil
}

// runInventoryScans scans for reserved-metadata objects created while enforcement was not known to hold, and records a
// clearance after a clean scan. It only scans while enforcement is verified: a scan taken without it proves nothing.
func runInventoryScans(ctx context.Context, log logrus.FieldLogger, tracker *rollout.Tracker, checker *readiness.Checker,
	lister rollout.Lister, metrics *nvcametrics.Metrics) {
	var lastSuspects string
	scan := func() {
		if !checker.Status().EnforcementVerified {
			metrics.SetUtilsProvenanceInventory(false, 0)
			return
		}
		suspects, err := tracker.Scan(ctx, lister)
		if err != nil {
			log.WithError(err).Warn("Utils provenance inventory scan failed")
			metrics.SetUtilsProvenanceInventory(false, 0)
			return
		}
		cleared, _ := tracker.State(ctx).Cleared()
		metrics.SetUtilsProvenanceInventory(cleared, len(suspects))
		names := make([]string, 0, len(suspects))
		for _, s := range suspects {
			names = append(names, s.String())
		}
		if key := strings.Join(names, ";"); key != lastSuspects {
			lastSuspects = key
			if len(suspects) > 0 {
				log.WithFields(logrus.Fields{"suspects": names}).Warn("Utils provenance inventory found objects with reserved metadata " +
					"created while enforcement was not verified. Delete and recreate them, the rollout stays closed until none remain")
			} else if cleared {
				log.Info("Utils provenance inventory cleared")
			}
		}
	}
	t := time.NewTicker(inventoryScanInterval)
	defer t.Stop()
	scan()
	for {
		select {
		case <-ctx.Done():
			metrics.SetUtilsProvenanceInventory(false, 0)
			return
		case <-t.C:
			scan()
		}
	}
}

// utilsReplicasFromEnv returns the requested utils replicas per instance. When the env is unset or invalid it returns
// the default: 2 when utils HA is enabled, 1 otherwise.
func utilsReplicasFromEnv(log logrus.FieldLogger, haEnabled bool) int32 {
	def := int32(1)
	if haEnabled {
		def = 2
	}
	v := os.Getenv(types.UtilsReplicasEnv)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil || n < 1 {
		log.WithFields(logrus.Fields{"env": types.UtilsReplicasEnv, "value": v, "default": def}).
			Warn("Invalid utils replicas, using the default")
		return def
	}
	return int32(n)
}

// miniServiceTrust gives the inventory scan the template digests NVCA recorded in MiniService status. Instance service
// accounts cannot write MiniService status, so the digests are trusted desired state.
type miniServiceTrust struct{ reader client.Reader }

// UtilsTrust implements rollout.Trust.
func (m miniServiceTrust) UtilsTrust(ctx context.Context, namespace string) (rollout.TrustedDigests, bool, error) {
	list := &v1alpha1.MiniServiceList{}
	if err := m.reader.List(ctx, list); err != nil {
		return rollout.TrustedDigests{}, false, err
	}
	for i := range list.Items {
		if list.Items[i].Spec.Namespace != namespace {
			continue
		}
		t, ok := mscontroller.UtilsHATrust(&list.Items[i])
		return rollout.TrustedDigests{
			PodSpec: t.PodSpecDigest, Containers: t.ContainersDigest, DeploymentSpec: t.DeploymentSpecDigest,
			Replicas: t.Replicas, DeploymentUID: t.DeploymentUID,
		}, ok, nil
	}
	return rollout.TrustedDigests{}, false, nil
}
