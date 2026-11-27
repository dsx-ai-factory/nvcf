// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func recoveryFixture(t *testing.T) (recoveryInventory, recoveryCheckpoint, routeContract, artifactLock, string, time.Time) {
	t.Helper()
	lock, err := loadLock("transitions/artifact-lock.json")
	if err != nil {
		t.Fatal(err)
	}
	route, err := loadRoute("transitions/umbrella-1.0.4-to-1.1.1.json", "umbrella-1.0.4-to-1.1.1", lock)
	if err != nil {
		t.Fatal(err)
	}
	ptr := func(n uint64) *uint64 { return &n }
	var baseline recoveryInventory
	for _, name := range []string{"CreateNvcaFunctionTaskStream", "TerminateNvcaStream", "requestToFunctionVersion_test", "stateful_session_lookup_test", "rq_test_00000000-0000-0000-0000-000000000001"} {
		stream := recoveryStream{Name: name, Config: map[string]any{"name": name, "storage": "memory", "num_replicas": json.Number("3")}}
		stream.State.Messages, stream.State.Bytes, stream.State.LastSeq, stream.State.Consumers = ptr(0), ptr(0), ptr(20), ptr(0)
		if strings.HasPrefix(name, "rq_") || strings.HasSuffix(name, "Stream") {
			consumer := recoveryConsumer{Name: name + "_consumer", Stream: name, Pending: ptr(0), AckPending: ptr(0), Redelivered: ptr(0)}
			consumer.Config = map[string]any{"durable_name": consumer.Name, "ack_policy": "explicit"}
			stream.Consumers, stream.State.Consumers = []recoveryConsumer{consumer}, ptr(1)
		}
		baseline.Streams = append(baseline.Streams, stream)
	}
	raw, _ := json.Marshal(baseline)
	baselineSHA := checksum(raw)
	now := time.Now().UTC()
	checkpoint := recoveryCheckpoint{SchemaVersion: 1, Route: route.ID, ContractSHA256: route.SHA256, LockSHA256: lock.SHA256, BaselineSHA256: baselineSHA, ServerImageDigest: checksum([]byte("native-platform"))}
	checkpoint.Barrier.ID, checkpoint.Barrier.RecordedAt = "paused-source", now.Add(-3*time.Second)
	checkpoint.Barrier.CallersPaused, checkpoint.Barrier.WritersPaused = true, true
	for _, elapsed := range []time.Duration{-2 * time.Second, -time.Second} {
		var copy recoveryInventory
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&copy); err != nil {
			t.Fatal(err)
		}
		checkpoint.Samples = append(checkpoint.Samples, recoverySample{ObservedAt: now.Add(elapsed), Streams: copy.Streams})
	}
	return baseline, checkpoint, route, lock, baselineSHA, now
}

func TestDrainedRecoveryRequiresCompleteStableEvidence(t *testing.T) {
	baseline, checkpoint, route, lock, digest, now := recoveryFixture(t)
	if err := validateRecoveryCheckpoint(baseline, checkpoint, route, lock, digest, now); err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(*recoveryInventory, *recoveryCheckpoint, *routeContract){
		"historical-route":   func(_ *recoveryInventory, c *recoveryCheckpoint, _ *routeContract) { c.Route = "1.0.4-to-1.1.5" },
		"undeclared-profile": func(_ *recoveryInventory, _ *recoveryCheckpoint, r *routeContract) { r.RecoveryMode = "" },
		"active-callers":     func(_ *recoveryInventory, c *recoveryCheckpoint, _ *routeContract) { c.Barrier.CallersPaused = false },
		"active-writers":     func(_ *recoveryInventory, c *recoveryCheckpoint, _ *routeContract) { c.Barrier.WritersPaused = false },
		"wrong-contract": func(_ *recoveryInventory, c *recoveryCheckpoint, _ *routeContract) {
			c.ContractSHA256 = checksum([]byte("other"))
		},
		"missing-image-digest": func(_ *recoveryInventory, c *recoveryCheckpoint, _ *routeContract) { c.ServerImageDigest = "" },
		"nonempty-stream": func(_ *recoveryInventory, c *recoveryCheckpoint, _ *routeContract) {
			*c.Samples[0].Streams[0].State.Messages = 1
		},
		"missing-count": func(_ *recoveryInventory, c *recoveryCheckpoint, _ *routeContract) {
			c.Samples[0].Streams[0].State.Bytes = nil
		},
		"unacknowledged": func(_ *recoveryInventory, c *recoveryCheckpoint, _ *routeContract) {
			*c.Samples[0].Streams[0].Consumers[0].AckPending = 1
		},
		"pending-consumer": func(_ *recoveryInventory, c *recoveryCheckpoint, _ *routeContract) {
			*c.Samples[0].Streams[0].Consumers[0].Pending = 1
		},
		"redelivery": func(_ *recoveryInventory, c *recoveryCheckpoint, _ *routeContract) {
			*c.Samples[0].Streams[0].Consumers[0].Redelivered = 1
		},
		"missing-consumer": func(_ *recoveryInventory, c *recoveryCheckpoint, _ *routeContract) {
			c.Samples[0].Streams[0].Consumers = nil
		},
		"changed-replicas": func(_ *recoveryInventory, c *recoveryCheckpoint, _ *routeContract) {
			c.Samples[0].Streams[0].Config["num_replicas"] = json.Number("1")
		},
		"changed-ack-policy": func(_ *recoveryInventory, c *recoveryCheckpoint, _ *routeContract) {
			c.Samples[0].Streams[0].Consumers[0].Config["ack_policy"] = "none"
		},
		"advancing-sequence": func(_ *recoveryInventory, c *recoveryCheckpoint, _ *routeContract) {
			*c.Samples[1].Streams[0].State.LastSeq++
		},
		"one-observation": func(_ *recoveryInventory, c *recoveryCheckpoint, _ *routeContract) { c.Samples = c.Samples[:1] },
		"stale": func(_ *recoveryInventory, c *recoveryCheckpoint, _ *routeContract) {
			c.Samples[0].ObservedAt = now.Add(-time.Hour)
		},
		"before-pause": func(_ *recoveryInventory, c *recoveryCheckpoint, _ *routeContract) {
			c.Samples[0].ObservedAt = c.Barrier.RecordedAt.Add(-time.Second)
		},
		"unreviewed-stream": func(b *recoveryInventory, _ *recoveryCheckpoint, _ *routeContract) {
			b.Streams[0].Name = "unreviewed"
			b.Streams[0].Config["name"] = "unreviewed"
			b.Streams[0].Consumers[0].Stream = "unreviewed"
		},
		"missing-family": func(b *recoveryInventory, _ *recoveryCheckpoint, _ *routeContract) { b.Streams = b.Streams[1:] },
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			b, c, r, l, sha, at := recoveryFixture(t)
			change(&b, &c, &r)
			if err := validateRecoveryCheckpoint(b, c, r, l, sha, at); err == nil {
				t.Fatal("accepted an unsafe or incomplete recovery checkpoint")
			}
		})
	}
}

func TestRecoveryCLIRecordsCheckpointWithoutQualifyingRoute(t *testing.T) {
	baseline, checkpoint, route, _, _, _ := recoveryFixture(t)
	root := t.TempDir()
	b, c, out := filepath.Join(root, "baseline.json"), filepath.Join(root, "checkpoint.json"), filepath.Join(root, "report")
	if err := writeJSON(b, baseline); err != nil {
		t.Fatal(err)
	}
	checksum, _ := fileChecksum(b)
	checkpoint.BaselineSHA256 = checksum
	if err := writeJSON(c, checkpoint); err != nil {
		t.Fatal(err)
	}
	args := []string{"--contracts", "transitions", "--route", route.ID, "--baseline", b, "--checkpoint", c, "--out", out}
	if err := runRecoveryCheck(args); err != nil {
		t.Fatal(err)
	}
	var report map[string]any
	if err := readJSON(filepath.Join(out, "record.json"), &report); err != nil {
		t.Fatal(err)
	}
	if report["qualification"] != "unqualified" || report["full_stack_restore_required"] != true || report["status"] != "drained-checkpoint-recorded" {
		t.Fatal("checkpoint was promoted to recovery qualification")
	}
	if err := runRecoveryCheck(args); err == nil {
		t.Fatal("overwrote an existing recovery report")
	}
	for _, path := range []string{out, filepath.Join(out, "record.json")} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm()&0077 != 0 {
			t.Fatal("recovery evidence is not owner-only")
		}
	}
}

func TestRecoveryRequiresTheCompleteQueueRegion(t *testing.T) {
	for _, tc := range []struct {
		name, region, queueRegion string
		valid                     bool
	}{
		{"matching", "test", "test", true},
		{"matching-underscore", "test_other", "test_other", true},
		{"shared-prefix", "test", "test_other", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			baseline, checkpoint, route, lock, _, now := recoveryFixture(t)
			rename := func(streams []recoveryStream) {
				for i := range streams {
					s := &streams[i]
					switch {
					case strings.HasPrefix(s.Name, "requestToFunctionVersion_"):
						s.Name = "requestToFunctionVersion_" + tc.region
					case strings.HasPrefix(s.Name, "stateful_session_lookup_"):
						s.Name = "stateful_session_lookup_" + tc.region
					case strings.HasPrefix(s.Name, "rq_"):
						s.Name = "rq_" + tc.queueRegion + "_" + strings.TrimPrefix(s.Name, "rq_test_")
					}
					s.Config["name"] = s.Name
					for j := range s.Consumers {
						c := &s.Consumers[j]
						c.Stream, c.Name = s.Name, s.Name+"_consumer"
						c.Config["durable_name"] = c.Name
					}
				}
			}
			rename(baseline.Streams)
			for i := range checkpoint.Samples {
				rename(checkpoint.Samples[i].Streams)
			}
			raw, err := json.Marshal(baseline)
			if err != nil {
				t.Fatal(err)
			}
			digest := checksum(raw)
			checkpoint.BaselineSHA256 = digest
			err = validateRecoveryCheckpoint(baseline, checkpoint, route, lock, digest, now)
			if tc.valid && err != nil {
				t.Fatal(err)
			}
			if !tc.valid && (err == nil || !strings.Contains(err.Error(), "queue region")) {
				t.Fatalf("accepted a queue from a different complete region: %v", err)
			}
		})
	}
}
