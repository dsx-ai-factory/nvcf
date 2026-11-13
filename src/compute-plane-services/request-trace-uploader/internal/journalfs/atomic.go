// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package journalfs

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrNotSynced reports a record that was renamed into place but whose
// directory sync failed. The new record is what a reader sees, so the store
// keeps it in memory and the change has taken effect; only its survival of
// a power loss is uncertain, which is the same as a crash before the
// rename. A caller treats the change as made.
var ErrNotSynced = errors.New("journal record is in place but its directory was not synced")

// writeJSONAtomic replaces dir/file with value: write a temporary file, sync
// it, rename it over the old file, then sync the directory so the rename
// itself survives a power loss. A failed directory sync wraps ErrNotSynced.
func writeJSONAtomic(dir, file string, value any, h hooks) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode journal record: %w", err)
	}
	final := filepath.Join(dir, file)
	tmp := final + tmpExt
	if err := writeAndSync(tmp, data, h); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if h.beforeRename != nil {
		if err := h.beforeRename(file); err != nil {
			return err
		}
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename journal record into place: %w", err)
	}
	h.emit("rename", final)
	if err := syncDirWith(h, dir); err != nil {
		return fmt.Errorf("%w: %w", ErrNotSynced, err)
	}
	return nil
}

// syncDirWith syncs dir and reports the step to the test hooks.
func syncDirWith(h hooks, dir string) error {
	if h.beforeSyncDir != nil {
		if err := h.beforeSyncDir(dir); err != nil {
			return err
		}
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	h.emit("sync-dir", dir)
	return nil
}

// mkdirDurable creates dir and any missing parents, then syncs the parent of
// each directory it created, top down, so the new links survive a power
// loss before anything is moved into them.
func mkdirDurable(h hooks, dir string) error {
	var created []string
	for d := filepath.Clean(dir); ; d = filepath.Dir(d) {
		if _, err := os.Stat(d); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("stat journal directory: %w", err)
		}
		created = append(created, d)
		if filepath.Dir(d) == d {
			break
		}
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create journal directory: %w", err)
	}
	for i := len(created) - 1; i >= 0; i-- {
		if err := syncDirWith(h, filepath.Dir(created[i])); err != nil {
			return err
		}
	}
	return nil
}

func writeAndSync(path string, data []byte, h hooks) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
	if err != nil {
		return fmt.Errorf("create journal temporary file: %w", err)
	}
	_, writeErr := f.Write(data)
	syncErr := f.Sync()
	if writeErr == nil && syncErr == nil {
		h.emit("sync-file", path)
	}
	closeErr := f.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return fmt.Errorf("write journal temporary file: %w", err)
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open journal directory for sync: %w", err)
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return fmt.Errorf("sync journal directory: %w", err)
	}
	return nil
}

// removeDurably deletes path and syncs its directory. A missing file is not
// an error, so a step that a crash interrupted can be repeated; the
// directory is still synced, because the earlier removal may not be durable.
func removeDurably(h hooks, path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove journal file: %w", err)
	}
	return syncDirWith(h, filepath.Dir(path))
}
