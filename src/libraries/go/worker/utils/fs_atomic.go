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
)

// WriteFileAtomic writes data to path so a reader never sees a partial file, and so several writers of the same path,
// for example the replicas of an HA instance writing a token to a shared volume, never collide. Each call writes its own
// uniquely named temporary file in the same directory, sets mode on it, and renames it over path. A rename is atomic
// within a directory on POSIX filesystems. The last rename wins, and every winner is a complete file.
//
// A fixed temporary name (path plus ".tmp") is not safe: two writers truncate and write the same temporary file, the
// first rename moves it, and the second rename fails with ENOENT.
func WriteFileAtomic(path string, data []byte, mode os.FileMode) (err error) {
	dir, base := filepath.Split(path)
	if dir == "" {
		dir = "."
	}
	f, err := os.CreateTemp(dir, base+".*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary file for %s: %w", path, err)
	}
	tmp := f.Name()
	defer func() {
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()
	if _, err = f.Write(data); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	// CreateTemp creates the file with mode 0600, so force the intended mode, for example world-readable so a non-root
	// reader can read it.
	if err = f.Chmod(mode); err != nil {
		return fmt.Errorf("chmod %s: %w", tmp, err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tmp, err)
	}
	if err = os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename %s to %s: %w", tmp, path, err)
	}
	return nil
}
