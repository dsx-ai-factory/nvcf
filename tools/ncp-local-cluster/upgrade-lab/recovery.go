// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"time"
)

const drainedRecoveryMode = "drained-memory-cold-restore"

type recoveryConsumer struct {
	Name        string         `json:"name"`
	Stream      string         `json:"stream_name"`
	Config      map[string]any `json:"config"`
	Pending     *uint64        `json:"num_pending"`
	AckPending  *uint64        `json:"num_ack_pending"`
	Redelivered *uint64        `json:"num_redelivered"`
}

type recoveryStream struct {
	Name   string         `json:"name"`
	Config map[string]any `json:"config"`
	State  struct {
		Messages  *uint64 `json:"messages"`
		Bytes     *uint64 `json:"bytes"`
		Consumers *uint64 `json:"consumer_count"`
		LastSeq   *uint64 `json:"last_seq"`
	} `json:"state"`
	Consumers []recoveryConsumer `json:"consumers"`
}

type recoveryInventory struct {
	Streams []recoveryStream `json:"streams"`
}

type recoverySample struct {
	ObservedAt time.Time        `json:"observed_at"`
	Streams    []recoveryStream `json:"streams"`
}

type recoveryCheckpoint struct {
	SchemaVersion     int    `json:"schema_version"`
	Route             string `json:"route"`
	ContractSHA256    string `json:"contract_sha256"`
	LockSHA256        string `json:"lock_sha256"`
	BaselineSHA256    string `json:"baseline_sha256"`
	ServerImageDigest string `json:"server_image_digest"`
	Barrier           struct {
		ID            string    `json:"id"`
		RecordedAt    time.Time `json:"recorded_at"`
		CallersPaused bool      `json:"callers_paused"`
		WritersPaused bool      `json:"writers_paused"`
	} `json:"barrier"`
	Samples []recoverySample `json:"samples"`
}

var recoveryQueueName = regexp.MustCompile(`^rq_([a-z0-9_-]+)_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func recoveryStreamsByName(streams []recoveryStream) (map[string]recoveryStream, error) {
	byName := map[string]recoveryStream{}
	for _, stream := range streams {
		if stream.Name == "" || stream.Config["name"] != stream.Name {
			return nil, errors.New("stream name is absent or differs from its native configuration")
		}
		if _, exists := byName[stream.Name]; exists {
			return nil, errors.New("duplicate stream in recovery inventory")
		}
		if stream.Config["storage"] != "memory" {
			return nil, errors.New("recovery profile requires separately reviewed memory streams")
		}
		replicas, ok := stream.Config["num_replicas"].(json.Number)
		count, err := replicas.Int64()
		if !ok || err != nil || count < 1 || count > 3 {
			return nil, errors.New("missing or unsupported native stream replica count")
		}
		if stream.State.Consumers == nil || *stream.State.Consumers != uint64(len(stream.Consumers)) {
			return nil, errors.New("consumer inventory is incomplete")
		}
		names := map[string]bool{}
		for _, consumer := range stream.Consumers {
			if consumer.Name == "" || consumer.Stream != stream.Name || names[consumer.Name] || consumer.Config["durable_name"] != consumer.Name {
				return nil, errors.New("consumer name, ownership or durable configuration is incomplete")
			}
			names[consumer.Name] = true
		}
		byName[stream.Name] = stream
	}
	if len(byName) == 0 {
		return nil, errors.New("recovery stream inventory is empty")
	}
	return byName, nil
}

func validateRecoveryCheckpoint(baseline recoveryInventory, checkpoint recoveryCheckpoint, route routeContract, lock artifactLock, baselineSHA string, now time.Time) error {
	if route.RecoveryMode != drainedRecoveryMode {
		return errors.New("route does not declare the drained-memory recovery profile")
	}
	if checkpoint.SchemaVersion != 1 || checkpoint.Route != route.ID || checkpoint.ContractSHA256 != route.SHA256 || checkpoint.LockSHA256 != lock.SHA256 || checkpoint.BaselineSHA256 != baselineSHA || !digestPattern.MatchString(checkpoint.ServerImageDigest) {
		return errors.New("recovery checkpoint does not match the route, baseline or immutable evidence")
	}
	if checkpoint.Barrier.ID == "" || !checkpoint.Barrier.CallersPaused || !checkpoint.Barrier.WritersPaused || checkpoint.Barrier.RecordedAt.IsZero() {
		return errors.New("record the caller and affected-writer pause before the recovery checkpoint")
	}
	expected, err := recoveryStreamsByName(baseline.Streams)
	if err != nil {
		return fmt.Errorf("baseline: %w", err)
	}
	region, queues := "", 0
	for name := range expected {
		switch {
		case strings.HasPrefix(name, "requestToFunctionVersion_"):
			if region != "" {
				return errors.New("multiple result-tracking regions require separate recovery review")
			}
			region = strings.TrimPrefix(name, "requestToFunctionVersion_")
		case name == "CreateNvcaFunctionTaskStream", name == "TerminateNvcaStream", strings.HasPrefix(name, "stateful_session_lookup_"):
		case recoveryQueueName.MatchString(name):
			queues++
		default:
			return errors.New("unknown stream family requires recovery review")
		}
	}
	if region == "" || queues == 0 || len(expected) != queues+4 {
		return errors.New("required request, session or management streams are missing")
	}
	for _, name := range []string{"CreateNvcaFunctionTaskStream", "TerminateNvcaStream", "stateful_session_lookup_" + region} {
		if _, exists := expected[name]; !exists {
			return errors.New("required request, session or management streams are missing")
		}
	}
	for name := range expected {
		if match := recoveryQueueName.FindStringSubmatch(name); len(match) > 0 && match[1] != region {
			return errors.New("function queue region differs from the recovery profile")
		}
	}
	if len(checkpoint.Samples) != 2 {
		return errors.New("two complete recovery observations are required")
	}
	previous := checkpoint.Barrier.RecordedAt
	var first map[string]recoveryStream
	for _, sample := range checkpoint.Samples {
		if !sample.ObservedAt.After(previous) || sample.ObservedAt.After(now.Add(time.Minute)) || now.Sub(sample.ObservedAt) > 5*time.Minute {
			return errors.New("recovery observations must be fresh and follow the pause barrier")
		}
		previous = sample.ObservedAt
		current, err := recoveryStreamsByName(sample.Streams)
		if err != nil {
			return err
		}
		if len(current) != len(expected) {
			return errors.New("stream inventory changed after the baseline")
		}
		for name, frozen := range expected {
			stream, exists := current[name]
			if !exists || !reflect.DeepEqual(frozen.Config, stream.Config) {
				return errors.New("stream configuration, storage or replication changed after the baseline")
			}
			if stream.State.Messages == nil || stream.State.Bytes == nil || stream.State.LastSeq == nil || *stream.State.Messages != 0 || *stream.State.Bytes != 0 {
				return errors.New("stream is not empty or lacks native drain counters")
			}
			consumers := map[string]recoveryConsumer{}
			for _, consumer := range stream.Consumers {
				if consumer.Pending == nil || consumer.AckPending == nil || consumer.Redelivered == nil || *consumer.Pending != 0 || *consumer.AckPending != 0 || *consumer.Redelivered != 0 {
					return errors.New("consumer has pending, unacknowledged or redelivered work")
				}
				consumers[consumer.Name] = consumer
			}
			if len(consumers) != len(frozen.Consumers) {
				return errors.New("consumer inventory changed after the baseline")
			}
			for _, consumer := range frozen.Consumers {
				if !reflect.DeepEqual(consumer.Config, consumers[consumer.Name].Config) {
					return errors.New("consumer identity or configuration changed after the baseline")
				}
			}
			if first != nil && *first[name].State.LastSeq != *stream.State.LastSeq {
				return errors.New("stream sequence advanced after writers were paused")
			}
		}
		first = current
	}
	return nil
}

func runRecoveryCheck(args []string) error {
	f := flag.NewFlagSet("check-recovery", flag.ContinueOnError)
	contracts := f.String("contracts", "tools/ncp-local-cluster/upgrade-lab/transitions", "transition contract directory")
	id := f.String("route", "", "exact route identity")
	baselinePath := f.String("baseline", "", "private native source stream inventory")
	checkpointPath := f.String("checkpoint", "", "private paused-source checkpoint JSON")
	out := f.String("out", "", "new private report directory")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *baselinePath == "" || *checkpointPath == "" || *out == "" {
		return errors.New("check-recovery requires --route, --baseline, --checkpoint and --out")
	}
	lock, err := loadLock(filepath.Join(*contracts, "artifact-lock.json"))
	if err != nil {
		return err
	}
	route, err := loadRoute(filepath.Join(*contracts, *id+".json"), *id, lock)
	if err != nil {
		return err
	}
	baselineRaw, err := os.ReadFile(*baselinePath)
	if err != nil {
		return err
	}
	checkpointRaw, err := os.ReadFile(*checkpointPath)
	if err != nil {
		return err
	}
	var baseline recoveryInventory
	var checkpoint recoveryCheckpoint
	decode := func(raw []byte, target any) error {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(target); err != nil {
			return err
		}
		if decoder.Decode(new(any)) != io.EOF {
			return errors.New("recovery input must contain one JSON object")
		}
		return nil
	}
	if decode(baselineRaw, &baseline) != nil || decode(checkpointRaw, &checkpoint) != nil {
		return errors.New("invalid private recovery inventory or checkpoint JSON")
	}
	if err := validateRecoveryCheckpoint(baseline, checkpoint, route, lock, checksum(baselineRaw), time.Now().UTC()); err != nil {
		return err
	}
	if err := os.Mkdir(*out, 0700); err != nil {
		return fmt.Errorf("recovery report directory must be new: %w", err)
	}
	report := map[string]any{"route": route.ID, "status": "drained-checkpoint-recorded", "qualification": "unqualified", "contract_sha256": route.SHA256, "lock_sha256": lock.SHA256, "baseline_sha256": checksum(baselineRaw), "checkpoint_sha256": checksum(checkpointRaw), "stream_count": len(baseline.Streams), "full_stack_restore_required": true, "scope": "Checks supplied native observations and pause declarations; does not establish that writers are stopped or that recovery passed."}
	if err := writeJSON(filepath.Join(*out, "record.json"), report); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"status": report["status"], "qualification": "unqualified", "stream_count": len(baseline.Streams)})
}
