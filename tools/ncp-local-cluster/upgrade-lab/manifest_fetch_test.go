// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func TestFetchVerifiesRemoteManifestBeforeInventories(t *testing.T) {
	lock, err := loadLock("transitions/artifact-lock.json")
	if err != nil {
		t.Fatal(err)
	}
	route, err := loadRoute("transitions/umbrella-1.0.1-to-1.0.4.json", "umbrella-1.0.1-to-1.0.4", lock)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := lock.selection("1.0.4")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("transitions/" + selected.Manifest.Path)
	if err != nil {
		t.Fatal(err)
	}
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "pinned-manifest", true: "changed-manifest"}[changed], func(t *testing.T) {
			calls := 0
			exec := func(_ context.Context, name string, args ...string) ([]byte, error) {
				calls++
				path := args[len(args)-1]
				if calls == 1 {
					want := "repos/" + lock.Repository + "/contents/deploy/releases/1.0.4/manifest.json?ref=" + selected.Manifest.SourceCommit
					if name != "gh" || path != want || !strings.Contains(strings.Join(args, " "), "application/vnd.github.raw+json") {
						t.Fatalf("unexpected manifest request: %s %v", name, args)
					}
					if changed {
						return append(append([]byte(nil), raw...), '\n'), nil
					}
					return raw, nil
				}
				if !strings.Contains(path, "/releases/assets/") {
					t.Fatalf("unexpected inventory request: %s", path)
				}
				return nil, errors.New("inventory sentinel")
			}
			err := fetchInputsWithExecutor(context.Background(), lock, route, t.TempDir(), time.Second, exec)
			if changed {
				if err == nil || !strings.Contains(err.Error(), "remote umbrella manifest digest mismatch") || calls != 1 {
					t.Fatalf("changed manifest reached inventory fetch: calls=%d err=%v", calls, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "GitHub input download failed") || calls != 2 {
				t.Fatalf("pinned manifest did not precede inventory: calls=%d err=%v", calls, err)
			}
		})
	}
}
