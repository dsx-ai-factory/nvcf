// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCommittedProvenanceBindsCurrentReports(t *testing.T) {
	var provenance struct {
		Scope   string `json:"version_scope"`
		LockSHA string `json:"artifact_lock_sha256"`
		Reports []struct {
			Route       string `json:"route"`
			ContractSHA string `json:"contract_sha256"`
			ReportSHA   string `json:"component_report_sha256"`
		} `json:"reports"`
	}
	if err := readJSON("transitions/artifact-provenance.json", &provenance); err != nil {
		t.Fatal(err)
	}
	lock, err := loadLock("transitions/artifact-lock.json")
	if err != nil {
		t.Fatal(err)
	}
	if provenance.Scope != lock.VersionScope || provenance.LockSHA != lock.SHA256 || len(provenance.Reports) != 2 {
		t.Fatal("provenance does not bind the current lock and routes")
	}
	for _, report := range provenance.Reports {
		route, err := loadRoute(filepath.Join("transitions", report.Route+".json"), report.Route, lock)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(filepath.Join("transitions", report.Route+"-components.md"))
		if err != nil {
			t.Fatal(err)
		}
		if report.ContractSHA != route.SHA256 || report.ReportSHA != checksum(raw) {
			t.Fatalf("stale report binding: %s", report.Route)
		}
	}
}
