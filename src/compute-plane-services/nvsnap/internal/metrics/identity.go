// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package metrics

import "github.com/prometheus/client_golang/prometheus"

// IdentityRestoreDecisions counts the identity restore's decisions
// (internal/agent/identity_etcd.go): a capture recorded with its etcd
// identity or refused, the dumped source's outside connections reset or
// not, a restore refused (lease live, identity held, an error, the flag
// off), the lease granted again, and how the agent's keep-alive ended.
var IdentityRestoreDecisions = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Name:      "identity_restore_decisions_total",
	Help:      "Identity restore decisions: captured, capture_refused, peers_reset, peers_reset_failed, refused_*, regranted, keepalive_*, restore_aborted, keeper_agent_stopped and keys_write_failed.",
}, []string{"decision"})

// IdentityDecisions are every label value of IdentityRestoreDecisions,
// created at zero on registration so a missing series is never read as
// none happened.
var IdentityDecisions = []string{
	"captured", "capture_refused", "peers_reset", "peers_reset_failed",
	"refused_live_lease", "refused_held", "refused_error", "refused_disabled",
	"regranted", "keepalive_handoff", "keepalive_handoff_timeout", "keepalive_lease_lost",
	"restore_aborted", "keeper_agent_stopped", "keys_write_failed",
}
