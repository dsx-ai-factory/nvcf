// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package record

import "syscall"

// openNoFollow refuses a final symlink and keeps open from waiting on a pipe.
const openNoFollow = syscall.O_NOFOLLOW | syscall.O_NONBLOCK
