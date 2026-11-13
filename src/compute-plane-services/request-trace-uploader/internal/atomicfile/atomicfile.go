// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package atomicfile writes a file under a temporary name and publishes it
// under its final name only once it is complete and synced, so a reader never
// sees a partial file. Every writer that publishes a local file uses it.
//
// Errors carry no file system paths, because callers log them.
package atomicfile

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// syncDir syncs a directory so a rename in it is durable. Tests replace it.
var syncDir = func(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(handle.Sync(), handle.Close())
}

// File is a file being written under a temporary name.
type File struct {
	file *os.File
}

// Create starts a file in dir. pattern is the temporary name, as for
// os.CreateTemp; callers choose one that recovery can find and remove.
func Create(dir, pattern string) (*File, error) {
	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, Pathless(err)
	}
	return &File{file: file}, nil
}

// Write appends p.
func (f *File) Write(p []byte) (int, error) {
	n, err := f.file.Write(p)
	return n, Pathless(err)
}

// Publish syncs and closes the file, then renames it to path and syncs the
// directory. On any failure it removes the temporary file, and the file at
// path if the rename happened, so a failed Publish leaves neither name.
func (f *File) Publish(path string) error {
	err := f.file.Sync()
	if closeErr := f.file.Close(); err == nil {
		err = closeErr
	}
	renamed := false
	if err == nil {
		err = os.Rename(f.file.Name(), path)
		renamed = err == nil
	}
	if err == nil {
		err = syncDir(filepath.Dir(path))
	}
	if err == nil {
		return nil
	}
	cleanup := removeIfPresent(f.file.Name())
	if renamed {
		cleanup = errors.Join(cleanup, removeIfPresent(path))
	}
	return Pathless(errors.Join(err, cleanup))
}

// Discard closes and removes an unpublished file. It is safe to call more
// than once and after a Publish that failed.
func (f *File) Discard() error {
	closeErr := f.file.Close()
	if errors.Is(closeErr, os.ErrClosed) {
		closeErr = nil
	}
	return Pathless(errors.Join(closeErr, removeIfPresent(f.file.Name())))
}

func removeIfPresent(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// Pathless drops the path from a file system error, keeping the operation
// and the cause, so the error can be logged. Joined errors are rewritten one
// by one.
func Pathless(err error) error {
	if err == nil {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		parts := make([]error, 0, len(joined.Unwrap()))
		for _, part := range joined.Unwrap() {
			parts = append(parts, Pathless(part))
		}
		return errors.Join(parts...)
	}
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) {
		return fmt.Errorf("%s: %w", pathErr.Op, pathErr.Err)
	}
	var linkErr *os.LinkError
	if errors.As(err, &linkErr) {
		return fmt.Errorf("%s: %w", linkErr.Op, linkErr.Err)
	}
	return err
}
