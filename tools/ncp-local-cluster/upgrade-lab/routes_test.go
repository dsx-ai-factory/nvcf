// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestDeclaredRoutesRetainRequiredUmbrellaStop(t *testing.T) {
	lock, err := loadLock("transitions/artifact-lock.json")
	if err != nil {
		t.Fatal(err)
	}
	load := func(id string) routeContract {
		t.Helper()
		route, err := loadRoute(filepath.Join("transitions", id+".json"), id, lock)
		if err != nil {
			t.Fatal(err)
		}
		return route
	}
	first := load("umbrella-1.0.1-to-1.0.4")
	second := load("umbrella-1.0.4-to-1.1.1")
	if first.To != second.From || second.From != "1.0.4" {
		t.Fatal("declared transitions do not retain the required umbrella stop")
	}
	selection, err := lock.selection(second.To)
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Previous string `json:"previous_version"`
	}
	if err := readJSON(filepath.Join("transitions", selection.Manifest.Path), &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Previous == "" || manifest.Previous == second.From {
		t.Fatal("fixture must distinguish release ancestry from the required upgrade stop")
	}
	if _, err := lock.selection(manifest.Previous); err == nil {
		t.Fatal("release ancestry became an additional endpoint selection")
	}
}

func TestUndeclaredUmbrellaRoutesFailBeforeOutput(t *testing.T) {
	for _, id := range []string{"umbrella-1.0.4-to-1.1.0", "umbrella-1.0.1-to-1.1.1"} {
		t.Run(id, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "must-not-exist")
			err := run(context.Background(), []string{"audit", "--contracts", "transitions", "--route", id, "--inputs", t.TempDir(), "--out", out})
			if err == nil {
				t.Fatal("accepted an undeclared route")
			}
			if _, err := os.Stat(out); !os.IsNotExist(err) {
				t.Fatal("created output for an undeclared route")
			}
		})
	}
}
