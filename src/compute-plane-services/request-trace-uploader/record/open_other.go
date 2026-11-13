// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package record

// openNoFollow is empty where the platform has no such flags; openRegular
// still checks the type of the opened file.
const openNoFollow = 0
