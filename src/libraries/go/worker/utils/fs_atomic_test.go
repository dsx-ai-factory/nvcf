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

package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteFileAtomicWritesAndSetsMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	require.NoError(t, WriteFileAtomic(path, []byte("one"), 0o644))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "one", string(got))
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o644), info.Mode().Perm(), "the temporary file's 0600 must not leak into the result")

	// Replacing works, and the mode is applied again.
	require.NoError(t, WriteFileAtomic(path, []byte("two"), 0o640))
	got, _ = os.ReadFile(path)
	assert.Equal(t, "two", string(got))
	info, _ = os.Stat(path)
	assert.Equal(t, os.FileMode(0o640), info.Mode().Perm())
}

func TestWriteFileAtomicLeavesNoTemporaryFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	for i := 0; i < 5; i++ {
		require.NoError(t, WriteFileAtomic(path, []byte(fmt.Sprint(i)), 0o644))
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "only the target remains")
	assert.Equal(t, "token", entries[0].Name())
}

func TestWriteFileAtomicCleansUpOnFailure(t *testing.T) {
	dir := t.TempDir()
	// The target is a non-empty directory, so the rename fails after the temporary file was written.
	target := filepath.Join(dir, "token")
	require.NoError(t, os.MkdirAll(filepath.Join(target, "child"), 0o755))
	err := WriteFileAtomic(target, []byte("x"), 0o644)
	require.Error(t, err)

	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	for _, e := range entries {
		assert.False(t, strings.HasSuffix(e.Name(), ".tmp"), "a failed write must remove its temporary file, found %s", e.Name())
	}

	// A missing directory is an error, not a silent success.
	assert.Error(t, WriteFileAtomic(filepath.Join(dir, "missing", "token"), []byte("x"), 0o644))
}

// TestWriteFileAtomicConcurrentWritersNeverFailOrTearFiles is the regression for the fixed ".tmp" name. Many writers
// write the same path at once, as replicas sharing a volume would. With a shared temporary name some renames fail with
// ENOENT and a reader can see a file another writer is still writing. Here every call must succeed and every read must
// be one complete value.
func TestWriteFileAtomicConcurrentWritersNeverFailOrTearFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	require.NoError(t, WriteFileAtomic(path, []byte("seed-0000000000"), 0o644))

	const writers, perWriter = 8, 60
	values := map[string]bool{"seed-0000000000": true}
	for w := 0; w < writers; w++ {
		for i := 0; i < perWriter; i++ {
			values[fmt.Sprintf("writer%d-value%04d", w, i)] = true
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, writers*perWriter)
	stop := make(chan struct{})
	torn := make(chan string, 1)

	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			b, err := os.ReadFile(path)
			if err == nil && !values[string(b)] {
				select {
				case torn <- string(b):
				default:
				}
				return
			}
		}
	}()

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				errs <- WriteFileAtomic(path, []byte(fmt.Sprintf("writer%d-value%04d", w, i)), 0o644)
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	close(errs)

	for err := range errs {
		require.NoError(t, err, "no writer may fail because another is writing the same path")
	}
	select {
	case v := <-torn:
		t.Fatalf("a reader saw a partial or unknown value %q", v)
	default:
	}
	final, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.True(t, values[string(final)], "the final content is one complete value: %q", final)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 1, "no temporary files are left behind")
}

// TestFixedTemporaryNameCollides documents the bug the helper removes: writers that share path+".tmp" fail.
func TestFixedTemporaryNameCollides(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	fixedTmpWrite := func(data string) error {
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, []byte(data), 0o644); err != nil {
			return err
		}
		return os.Rename(tmp, path)
	}
	var failures int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if err := fixedTmpWrite("v"); err != nil {
					mu.Lock()
					failures++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	t.Logf("fixed temporary name: %d failed renames in 1600 writes", failures)
	// Not asserted as a hard failure count: it depends on scheduling. The point of the test is the log line and that the
	// atomic helper above has zero failures under the same load.
}
