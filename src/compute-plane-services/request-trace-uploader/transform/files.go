// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package transform

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/atomicfile"
)

// syncDir syncs a directory. A var so a test can see which directories are
// synced.
var syncDir = func(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(handle.Sync(), handle.Close())
}

// mkdirDurable creates dir and any missing parents, then syncs the parent of
// each directory above dir that it created, top down, and the parent of dir
// itself, so the new links survive a power loss before anything is written
// into them. dir's parent is synced even when dir exists, because an earlier
// attempt may have made dir and failed before its sync.
func mkdirDurable(dir string) error {
	dir = filepath.Clean(dir)
	var created []string
	for d := filepath.Dir(dir); ; d = filepath.Dir(d) {
		if _, err := os.Stat(d); err == nil {
			break
		} else if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		created = append(created, d)
		if filepath.Dir(d) == d {
			break
		}
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	for i := len(created) - 1; i >= 0; i-- {
		if err := syncDir(filepath.Dir(created[i])); err != nil {
			return err
		}
	}
	return syncDir(filepath.Dir(dir))
}

// RejectFile collects the raw bytes of rejected lines for local inspection.
// It creates its directory and file only when the first line is rejected, so
// a clean segment leaves nothing behind. The bytes can hold user data: the
// file is readable by its owner only and must never be exported.
type RejectFile struct {
	dir string
	out *atomicfile.File
	gz  *gzip.Writer
	err error
}

// NewRejectFile collects rejects into dir.
func NewRejectFile(dir string) *RejectFile { return &RejectFile{dir: dir} }

// Write appends raw rejected bytes.
func (r *RejectFile) Write(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.out == nil {
		if err := mkdirDurable(r.dir); err != nil {
			r.err = fmt.Errorf("create reject directory: %w", atomicfile.Pathless(err))
			return 0, r.err
		}
		out, err := atomicfile.Create(r.dir, ".reject.*.tmp")
		if err != nil {
			r.err = fmt.Errorf("create reject artifact: %w", err)
			return 0, r.err
		}
		r.out, r.gz = out, gzip.NewWriter(out)
	}
	n, err := r.gz.Write(p)
	if err != nil {
		r.err = fmt.Errorf("write reject artifact: %w", err)
	}
	return n, r.err
}

// Commit names the artifact <name>.jsonl.gz, where name identifies the
// segment, and reports whether anything was rejected.
func (r *RejectFile) Commit(name string) (bool, error) {
	if r.out == nil {
		return false, r.err
	}
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
		return false, fmt.Errorf("reject artifact name %q is not a plain file name", name)
	}
	if r.err != nil {
		return false, r.err
	}
	if err := r.gz.Close(); err != nil {
		return false, errors.Join(fmt.Errorf("finish reject artifact: %w", err), r.Discard())
	}
	if err := r.out.Publish(filepath.Join(r.dir, name+".jsonl.gz")); err != nil {
		r.out = nil
		return false, fmt.Errorf("publish reject artifact: %w", err)
	}
	r.out = nil
	return true, nil
}

// Discard removes an artifact that was not committed.
func (r *RejectFile) Discard() error {
	if r.out == nil {
		return nil
	}
	err := r.out.Discard()
	r.out = nil
	if err != nil {
		return fmt.Errorf("discard reject artifact: %w", err)
	}
	return nil
}
