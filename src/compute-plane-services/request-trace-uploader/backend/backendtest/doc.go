// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package backendtest provides test helpers for code that uses a
// backend.Client. Only tests import it; its Bazel target is testonly.
//
// Fake is an in-memory asynchronous backend whose submit and status outcomes
// a test scripts in advance. It enforces its declared capabilities, checks
// declared file sizes, and counts files that land twice, so tests can drive
// the core through accepted, refused, lost-response, slow, stuck, failed and
// gone jobs without a real destination.
package backendtest
