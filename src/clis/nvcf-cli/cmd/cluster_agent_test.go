/*
SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

package cmd

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"nvcf-cli/internal/client"
	"nvcf-cli/internal/clusteragent"

	"github.com/spf13/cobra"
)

func TestMatchICMSCluster(t *testing.T) {
	clusters := []client.ICMSCluster{
		{ClusterID: "id-1", ClusterName: "alpha"},
		{ClusterID: "id-2", ClusterName: "beta"},
	}

	t.Run("matches by id", func(t *testing.T) {
		got := matchICMSCluster(clusters, "id-2", "")
		if got == nil || got.ClusterName != "beta" {
			t.Fatalf("got %+v, want beta", got)
		}
	})

	t.Run("prefers id over name", func(t *testing.T) {
		got := matchICMSCluster(clusters, "id-1", "beta")
		if got == nil || got.ClusterName != "alpha" {
			t.Fatalf("got %+v, want alpha (id wins)", got)
		}
	})

	t.Run("falls back to name", func(t *testing.T) {
		got := matchICMSCluster(clusters, "", "beta")
		if got == nil || got.ClusterID != "id-2" {
			t.Fatalf("got %+v, want id-2", got)
		}
	})

	t.Run("id not found does not fall back to name", func(t *testing.T) {
		got := matchICMSCluster(clusters, "missing-id", "alpha")
		if got != nil {
			t.Fatalf("got %+v, want nil: ID miss must not fall back to name", got)
		}
	})

	t.Run("no match", func(t *testing.T) {
		if got := matchICMSCluster(clusters, "missing", "missing"); got != nil {
			t.Fatalf("got %+v, want nil", got)
		}
	})
}

func TestEnrichStatusFromSISSkipsWithoutNCAID(t *testing.T) {
	info := enrichStatusFromICMS(&cobra.Command{}, context.Background(), "", &clusteragent.AgentStatus{})
	if info == nil || info.Available {
		t.Fatalf("expected unavailable ICMSInfo, got %+v", info)
	}
	if !strings.Contains(info.Note, "nca-id") {
		t.Errorf("note = %q, want a hint about --nca-id", info.Note)
	}
}

func TestBuildListOptions(t *testing.T) {
	newCmd := func() *cobra.Command {
		c := &cobra.Command{}
		c.Flags().String(flagNamespace, "", "")
		return c
	}

	t.Run("defaults to no filter", func(t *testing.T) {
		clusterAgentFlags.phase = ""
		opts, err := buildListOptions(newCmd())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if opts.PhaseFilter != "" {
			t.Errorf("got %+v, want empty PhaseFilter", opts)
		}
	})

	t.Run("accepts lowercase phase", func(t *testing.T) {
		clusterAgentFlags.phase = "draining"
		defer func() { clusterAgentFlags.phase = "" }()
		opts, err := buildListOptions(newCmd())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if opts.PhaseFilter != clusteragent.PhaseDraining {
			t.Errorf("PhaseFilter = %q, want DRAINING", opts.PhaseFilter)
		}
	})

	t.Run("rejects unknown phase", func(t *testing.T) {
		clusterAgentFlags.phase = "bogus"
		defer func() { clusterAgentFlags.phase = "" }()
		if _, err := buildListOptions(newCmd()); err == nil {
			t.Fatal("expected error for invalid phase")
		}
	})

	t.Run("accepts FAILED phase", func(t *testing.T) {
		clusterAgentFlags.phase = "FAILED"
		defer func() { clusterAgentFlags.phase = "" }()
		opts, err := buildListOptions(newCmd())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if opts.PhaseFilter != clusteragent.PhaseFailed {
			t.Errorf("PhaseFilter = %q, want FAILED", opts.PhaseFilter)
		}
	})
}

func TestClusterAgentHumanReadableOutput(t *testing.T) {
	tests := []struct {
		name string
		run  func()
		want []string
	}{
		{
			name: "status with GPU and unavailable control plane",
			run: func() {
				printAgentStatus(&clusteragent.AgentStatus{
					ClusterName: "edge", ClusterID: "c1", Namespace: "nvca-operator",
					GPU: []clusteragent.GPUUsage{{Name: "A100", Capacity: 8, Available: 3, Allocated: 5}},
					ControlPlane: &clusteragent.ICMSInfo{Note: "enrichment skipped"},
				})
			},
			want: []string{"edge (c1)", "A100", "8", "3", "5", "enrichment skipped"},
		},
		{
			name: "empty status defaults",
			run:  func() { printAgentStatus(&clusteragent.AgentStatus{}) },
			want: []string{"(none reported)", "unavailable", "NVCA Status"},
		},
		{
			name: "empty scheduled function list",
			run:  func() { printScheduledFunctions(nil) },
			want: []string{"No functions scheduled on this cluster."},
		},
		{
			name: "scheduled function row",
			run: func() {
				printScheduledFunctions([]clusteragent.ScheduledFunction{{
					FunctionID: "f1", FunctionVersionID: "v1", Namespace: "functions",
					Phase: clusteragent.PhaseActive, InstanceCount: 2, RequestStatus: "Ready",
				}})
			},
			want: []string{"FUNCTION ID", "f1", "v1", "functions", "ACTIVE", "2", "Ready", "Total: 1 functions"},
		},
		{
			name: "function detail without instances",
			run:  func() { printFunctionDetail(&clusteragent.FunctionDetail{ScheduledFunction: clusteragent.ScheduledFunction{FunctionID: "f1"}}) },
			want: []string{"Function ID:         f1", "Version ID:          -", "(none)"},
		},
		{
			name: "function detail with reconcile error and instance",
			run: func() {
				printFunctionDetail(&clusteragent.FunctionDetail{
					ScheduledFunction: clusteragent.ScheduledFunction{FunctionID: "f2", Action: "Update", Phase: clusteragent.PhaseFailed},
					LastReconcileError: "failed", ReconcileErrors: 3,
					Instances:          []clusteragent.Instance{{ID: "i1", Status: "Running"}},
				})
			},
			want: []string{"Reconcile Errors:    3", "Last Reconcile Err:  failed", "INSTANCE ID", "i1", "Running"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := captureClusterAgentStdout(t, tt.run)
			for _, want := range tt.want {
				if !strings.Contains(got, want) {
					t.Errorf("output missing %q:\n%s", want, got)
				}
			}
		})
	}
}

func captureClusterAgentStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		var b bytes.Buffer
		_, _ = io.Copy(&b, r)
		done <- b.String()
	}()
	fn()
	if err := w.Close(); err != nil {
		t.Fatalf("close stdout writer: %v", err)
	}
	os.Stdout = old
	return <-done
}
