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

package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/dsx-ai-factory/nvcf/src/libraries/go/worker/test/testutils"
)

// TestPurgeFromStream_MissingStreamReturnsError checks that an error in the
// JetStream API reply is returned rather than dropped.
func TestPurgeFromStream_MissingStreamReturnsError(t *testing.T) {
	setupLogger()
	cluster, err := testutils.NewNatsSuperCluster(t)
	require.NoError(t, err)
	defer cluster.Shutdown()

	nc, err := nats.Connect(cluster.Clusters[0].Servers[0].ClientURL())
	require.NoError(t, err)
	defer nc.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	err = PurgeFromStream(ctx, nc, "no_such_stream", jetstream.WithPurgeSubject("no.such.subject"))
	var apiErr *jetstream.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, jetstream.JSErrCodeStreamNotFound, apiErr.ErrorCode)
}
