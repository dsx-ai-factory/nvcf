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

package cassandra

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/control-plane-services/event-ledger/internal/observability/logging"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/opentelemetry-go-extra/otelzap"
	"go.uber.org/zap"

	"github.com/dsx-ai-factory/nvcf/src/control-plane-services/event-ledger/internal/config"
	"github.com/dsx-ai-factory/nvcf/src/control-plane-services/event-ledger/internal/data_access"
)

func TestFilteredStatsTableNameCompatibility(t *testing.T) {
	assert.Equal(t, "stats_v3_ngc", filteredStatsV3Table)
}

func TestUpsertFilteredStatsV3KeepsLatestEvent(t *testing.T) {
	session := getTestSession(t)
	if session == nil {
		t.Skip("Cassandra not available for testing")
	}

	handler := &CassandraHandler{session: session}
	logger := otelzap.New(zap.NewNop())
	ctx := logging.AttachLoggerToContext(context.Background(), logger)

	_ = session.Query("TRUNCATE " + filteredStatsV3Table).Exec()
	t.Cleanup(func() { _ = session.Query("TRUNCATE " + filteredStatsV3Table).Exec() })

	createdAt := time.Now().Add(-2 * time.Minute).Truncate(time.Millisecond)
	latestTimestamp := createdAt.Add(2 * time.Minute)
	staleTimestamp := createdAt.Add(time.Minute)

	require.NoError(t, handler.UpsertFilteredStatsV3(ctx, "ns-latest", "ctx-1", "pod.pending", "test", createdAt, "", ""))
	require.NoError(t, handler.UpsertFilteredStatsV3(ctx, "ns-latest", "ctx-1", "pod.ready", "test", latestTimestamp, "", ""))
	require.NoError(t, handler.UpsertFilteredStatsV3(ctx, "ns-latest", "ctx-1", "pod.starting", "test", staleTimestamp, "", ""))

	var eventName string
	var storedTimestamp time.Time
	var storedCreatedAt time.Time
	err := session.Query(
		`SELECT event_name, timestamp, created_at FROM stats_v3_ngc WHERE namespace = ? AND context = ?`,
		"ns-latest",
		"ctx-1",
	).Scan(&eventName, &storedTimestamp, &storedCreatedAt)
	require.NoError(t, err)
	assert.Equal(t, "pod.ready", eventName)
	assert.Equal(t, latestTimestamp.UTC(), storedTimestamp.UTC().Truncate(time.Millisecond))
	assert.Equal(t, createdAt.UTC(), storedCreatedAt.UTC().Truncate(time.Millisecond))
}

func TestBulkUpsertEventsV3(t *testing.T) {
	session := getTestSession(t)
	if session == nil {
		t.Skip("Cassandra not available for testing")
	}

	handler := &CassandraHandler{session: session}
	logger := otelzap.New(zap.NewNop())
	ctx := logging.AttachLoggerToContext(context.Background(), logger)

	_ = session.Query("TRUNCATE events_v3").Exec()
	t.Cleanup(func() { _ = session.Query("TRUNCATE events_v3").Exec() })

	now := time.Now().Truncate(time.Millisecond)

	t.Run("inserts multiple events across partitions", func(t *testing.T) {
		events := []data_access.EventV3UpsertRecord{
			{Namespace: "ns-bulk", Context: "ctx-1", EventName: "pod.ready", Source: "test", Details: []byte(`{}`), Timestamp: now},
			{Namespace: "ns-bulk", Context: "ctx-1", EventName: "pod.pending", Source: "test", Details: []byte(`{}`), Timestamp: now},
			{Namespace: "ns-bulk", Context: "ctx-2", EventName: "pod.ready", Source: "test", Details: []byte(`{}`), Timestamp: now},
		}

		err := handler.BulkUpsertEventsV3(ctx, events)
		require.NoError(t, err)

		var count int
		err = session.Query(`SELECT COUNT(*) FROM events_v3 WHERE namespace = ? AND context = ?`, "ns-bulk", "ctx-1").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 2, count, "ctx-1 should have 2 events")

		err = session.Query(`SELECT COUNT(*) FROM events_v3 WHERE namespace = ? AND context = ?`, "ns-bulk", "ctx-2").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "ctx-2 should have 1 event")
	})

	t.Run("preserves created_at on update", func(t *testing.T) {
		_ = session.Query("TRUNCATE events_v3").Exec()

		original := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
		first := []data_access.EventV3UpsertRecord{
			{Namespace: "ns-ca", Context: "ctx-1", EventName: "pod.ready", Source: "test", Details: []byte(`{}`), Timestamp: original},
		}
		require.NoError(t, handler.BulkUpsertEventsV3(ctx, first))

		updated := time.Now().Truncate(time.Millisecond)
		second := []data_access.EventV3UpsertRecord{
			{Namespace: "ns-ca", Context: "ctx-1", EventName: "pod.ready", Source: "test", Details: []byte(`{}`), Timestamp: updated},
		}
		require.NoError(t, handler.BulkUpsertEventsV3(ctx, second))

		var createdAt time.Time
		err := session.Query(`SELECT created_at FROM events_v3 WHERE namespace = ? AND context = ? AND event_name = ?`,
			"ns-ca", "ctx-1", "pod.ready").Scan(&createdAt)
		require.NoError(t, err)
		assert.Equal(t, original.UTC(), createdAt.UTC().Truncate(time.Millisecond), "created_at should remain from first insert")
	})

	t.Run("empty input is a no-op", func(t *testing.T) {
		err := handler.BulkUpsertEventsV3(ctx, nil)
		assert.NoError(t, err)
	})
}

func TestBulkUpsertStatsV3(t *testing.T) {
	session := getTestSession(t)
	if session == nil {
		t.Skip("Cassandra not available for testing")
	}

	handler := &CassandraHandler{session: session}
	logger := otelzap.New(zap.NewNop())
	ctx := logging.AttachLoggerToContext(context.Background(), logger)

	_ = session.Query("TRUNCATE stats_v3").Exec()
	t.Cleanup(func() { _ = session.Query("TRUNCATE stats_v3").Exec() })

	now := time.Now().Truncate(time.Millisecond)

	t.Run("inserts stats for multiple contexts", func(t *testing.T) {
		events := []data_access.EventV3UpsertRecord{
			{Namespace: "ns-stats", Context: "ctx-1", EventName: "pod.ready", Source: "test", Details: []byte(`{}`), Timestamp: now},
			{Namespace: "ns-stats", Context: "ctx-2", EventName: "pod.pending", Source: "test", Details: []byte(`{}`), Timestamp: now},
		}

		err := handler.BulkUpsertStatsV3(ctx, events)
		require.NoError(t, err)

		var count int
		err = session.Query(`SELECT COUNT(*) FROM stats_v3 WHERE namespace = ?`, "ns-stats").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 2, count, "should have one stats row per context")
	})

	t.Run("deduplicates to latest event per context", func(t *testing.T) {
		_ = session.Query("TRUNCATE stats_v3").Exec()

		older := now.Add(-time.Minute)
		events := []data_access.EventV3UpsertRecord{
			{Namespace: "ns-dedup", Context: "ctx-1", EventName: "pod.pending", Source: "test", Details: []byte(`{}`), Timestamp: older},
			{Namespace: "ns-dedup", Context: "ctx-1", EventName: "pod.ready", Source: "test", Details: []byte(`{}`), Timestamp: now},
		}

		err := handler.BulkUpsertStatsV3(ctx, events)
		require.NoError(t, err)

		var eventName string
		err = session.Query(`SELECT event_name FROM stats_v3 WHERE namespace = ? AND context = ?`, "ns-dedup", "ctx-1").Scan(&eventName)
		require.NoError(t, err)
		assert.Equal(t, "pod.ready", eventName, "should store the latest event_name")
	})

	t.Run("preserves created_at on update", func(t *testing.T) {
		_ = session.Query("TRUNCATE stats_v3").Exec()

		original := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
		first := []data_access.EventV3UpsertRecord{
			{Namespace: "ns-sca", Context: "ctx-1", EventName: "pod.pending", Source: "test", Details: []byte(`{}`), Timestamp: original},
		}
		require.NoError(t, handler.BulkUpsertStatsV3(ctx, first))

		updated := time.Now().Truncate(time.Millisecond)
		second := []data_access.EventV3UpsertRecord{
			{Namespace: "ns-sca", Context: "ctx-1", EventName: "pod.ready", Source: "test", Details: []byte(`{}`), Timestamp: updated},
		}
		require.NoError(t, handler.BulkUpsertStatsV3(ctx, second))

		var createdAt time.Time
		err := session.Query(`SELECT created_at FROM stats_v3 WHERE namespace = ? AND context = ?`, "ns-sca", "ctx-1").Scan(&createdAt)
		require.NoError(t, err)
		assert.Equal(t, original.UTC(), createdAt.UTC().Truncate(time.Millisecond), "created_at should remain from first insert")
	})

	t.Run("empty input is a no-op", func(t *testing.T) {
		err := handler.BulkUpsertStatsV3(ctx, nil)
		assert.NoError(t, err)
	})
}

func TestGrpcProxyRetentionInCassandra(t *testing.T) {
	keyspace := os.Getenv("EVENT_LEDGER_TTL_TEST_KEYSPACE")
	if keyspace == "" {
		t.Skip("set EVENT_LEDGER_TTL_TEST_KEYSPACE to a prepared, isolated test keyspace")
	}
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.One
	cluster.Timeout = 5 * time.Second
	cluster.ConnectTimeout = 5 * time.Second
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: "cassandra", Password: "cassandra"}
	session, err := cluster.CreateSession()
	require.NoError(t, err)
	defer session.Close()

	handler := &CassandraHandler{session: session}
	handler.ConfigureV3Retention(config.V3Config{GrpcProxyEventTTLSeconds: 3, GrpcProxyStatsTTLSeconds: 5})
	ctx := logging.AttachLoggerToContext(context.Background(), otelzap.New(zap.NewNop()))
	ns := "grpc-proxy-retention-" + uuid.NewString()
	now := time.Now().Truncate(time.Millisecond)
	proxy := data_access.EventV3UpsertRecord{Namespace: ns, Context: "proxy", EventName: "GrpcProxy.Attachment.Closed", Source: grpcProxySource, Details: []byte(`{}`), Timestamp: now}
	other := data_access.EventV3UpsertRecord{Namespace: ns, Context: "other", EventName: "ready", Source: "nvidia-spot", Details: []byte(`{}`), Timestamp: now}
	single := data_access.EventV3UpsertRecord{Namespace: ns, Context: "single", EventName: "GrpcProxy.Work.Dispatched", Source: grpcProxySource, Details: []byte(`{}`), Timestamp: now}
	defer func() {
		for _, event := range []data_access.EventV3UpsertRecord{proxy, other, single} {
			_ = session.Query("DELETE FROM events_v3 WHERE namespace = ? AND context = ?", ns, event.Context).Exec()
			_ = session.Query("DELETE FROM stats_v3 WHERE namespace = ? AND context = ?", ns, event.Context).Exec()
		}
	}()

	require.NoError(t, handler.BulkUpsertEventsV3(ctx, []data_access.EventV3UpsertRecord{proxy, other}))
	require.NoError(t, handler.BulkUpsertStatsV3(ctx, []data_access.EventV3UpsertRecord{proxy, other}))
	require.NoError(t, handler.UpsertEventV3(ctx, ns, single.Context, single.EventName, single.Source, single.Details, single.Timestamp))
	require.NoError(t, handler.UpsertStatsV3(ctx, ns, single.Context, single.EventName, single.Source, single.Timestamp))
	var eventTTL, statsTTL int
	require.NoError(t, session.Query("SELECT TTL(details) FROM events_v3 WHERE namespace = ? AND context = ? AND event_name = ?", ns, proxy.Context, proxy.EventName).Scan(&eventTTL))
	require.InDelta(t, 3, eventTTL, 1)
	require.NoError(t, session.Query("SELECT TTL(updated_at) FROM stats_v3 WHERE namespace = ? AND context = ?", ns, proxy.Context).Scan(&statsTTL))
	require.InDelta(t, 5, statsTTL, 1)
	var otherTTL *int
	require.NoError(t, session.Query("SELECT TTL(details) FROM events_v3 WHERE namespace = ? AND context = ? AND event_name = ?", ns, other.Context, other.EventName).Scan(&otherTTL))
	require.Nil(t, otherTTL)
	require.NoError(t, session.Query("SELECT TTL(updated_at) FROM stats_v3 WHERE namespace = ? AND context = ?", ns, other.Context).Scan(&otherTTL))
	require.Nil(t, otherTTL)
	require.NoError(t, session.Query("SELECT TTL(details) FROM events_v3 WHERE namespace = ? AND context = ? AND event_name = ?", ns, single.Context, single.EventName).Scan(&eventTTL))
	require.InDelta(t, 3, eventTTL, 1)
	require.NoError(t, session.Query("SELECT TTL(updated_at) FROM stats_v3 WHERE namespace = ? AND context = ?", ns, single.Context).Scan(&statsTTL))
	require.InDelta(t, 5, statsTTL, 1)

	time.Sleep(2 * time.Second)
	proxy.Timestamp = time.Now().Truncate(time.Millisecond)
	single.Timestamp = proxy.Timestamp
	require.NoError(t, handler.BulkUpsertEventsV3(ctx, []data_access.EventV3UpsertRecord{proxy}))
	require.NoError(t, handler.BulkUpsertStatsV3(ctx, []data_access.EventV3UpsertRecord{proxy}))
	require.NoError(t, handler.UpsertEventV3(ctx, ns, single.Context, single.EventName, single.Source, single.Details, single.Timestamp))
	require.NoError(t, handler.UpsertStatsV3(ctx, ns, single.Context, single.EventName, single.Source, single.Timestamp))
	time.Sleep(2 * time.Second)
	require.NoError(t, session.Query("SELECT TTL(details) FROM events_v3 WHERE namespace = ? AND context = ? AND event_name = ?", ns, proxy.Context, proxy.EventName).Scan(&eventTTL))
	require.Greater(t, eventTTL, 0, "updated event must outlive its original expiry")
	require.NoError(t, session.Query("SELECT TTL(created_at) FROM stats_v3 WHERE namespace = ? AND context = ?", ns, proxy.Context).Scan(&statsTTL))
	require.Greater(t, statsTTL, 0, "created_at must be refreshed with the stats row")
	require.NoError(t, session.Query("SELECT TTL(created_at) FROM stats_v3 WHERE namespace = ? AND context = ?", ns, single.Context).Scan(&statsTTL))
	require.Greater(t, statsTTL, 0, "single-row stats must refresh created_at")
	time.Sleep(4 * time.Second)
	require.ErrorIs(t, session.Query("SELECT details FROM events_v3 WHERE namespace = ? AND context = ? AND event_name = ?", ns, proxy.Context, proxy.EventName).Scan(new([]byte)), gocql.ErrNotFound)
	require.ErrorIs(t, session.Query("SELECT event_name FROM stats_v3 WHERE namespace = ? AND context = ?", ns, proxy.Context).Scan(new(string)), gocql.ErrNotFound)
	require.ErrorIs(t, session.Query("SELECT details FROM events_v3 WHERE namespace = ? AND context = ? AND event_name = ?", ns, single.Context, single.EventName).Scan(new([]byte)), gocql.ErrNotFound)
	require.ErrorIs(t, session.Query("SELECT event_name FROM stats_v3 WHERE namespace = ? AND context = ?", ns, single.Context).Scan(new(string)), gocql.ErrNotFound)
	require.NoError(t, session.Query("SELECT event_name FROM stats_v3 WHERE namespace = ? AND context = ?", ns, other.Context).Scan(new(string)))
}

func TestGrpcProxyStatsRetentionReplacesPermanentRow(t *testing.T) {
	keyspace := os.Getenv("EVENT_LEDGER_TTL_TEST_KEYSPACE")
	if keyspace == "" {
		t.Skip("set EVENT_LEDGER_TTL_TEST_KEYSPACE to a prepared, isolated test keyspace")
	}
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.One
	cluster.Timeout = 5 * time.Second
	cluster.ConnectTimeout = 5 * time.Second
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: "cassandra", Password: "cassandra"}
	session, err := cluster.CreateSession()
	require.NoError(t, err)
	defer session.Close()

	handler := &CassandraHandler{session: session}
	handler.ConfigureV3Retention(config.V3Config{GrpcProxyStatsTTLSeconds: 3})
	ctx := logging.AttachLoggerToContext(context.Background(), otelzap.New(zap.NewNop()))

	for _, table := range []string{statsV3Table, filteredStatsV3Table} {
		t.Run(table, func(t *testing.T) {
			ns := "grpc-proxy-transition-" + uuid.NewString()
			const eventContext = "legacy"
			t.Cleanup(func() {
				_ = session.Query("DELETE FROM "+table+" WHERE namespace = ? AND context = ?", ns, eventContext).Exec()
			})
			legacyTimestamp := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
			require.NoError(t, session.Query("INSERT INTO "+table+" (namespace, context, event_name, timestamp, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
				ns, eventContext, "legacy", legacyTimestamp, legacyTimestamp, legacyTimestamp).Exec())

			upsert := func(name string, timestamp time.Time) error {
				if table == statsV3Table {
					return handler.UpsertStatsV3(ctx, ns, eventContext, name, grpcProxySource, timestamp)
				}
				return handler.UpsertFilteredStatsV3(ctx, ns, eventContext, name, grpcProxySource, timestamp, "", "")
			}
			currentTimestamp := time.Now().Truncate(time.Millisecond)
			require.NoError(t, upsert("proxy", currentTimestamp))
			require.NoError(t, upsert("stale", legacyTimestamp))
			var name string
			require.NoError(t, session.Query("SELECT event_name FROM "+table+" WHERE namespace = ? AND context = ?", ns, eventContext).Scan(&name))
			require.Equal(t, "proxy", name)

			time.Sleep(4 * time.Second)
			require.ErrorIs(t, session.Query("SELECT event_name FROM "+table+" WHERE namespace = ? AND context = ?", ns, eventContext).Scan(&name), gocql.ErrNotFound)
			require.NoError(t, upsert("revived", time.Now().Truncate(time.Millisecond)))
			require.NoError(t, session.Query("SELECT event_name FROM "+table+" WHERE namespace = ? AND context = ?", ns, eventContext).Scan(&name))
			require.Equal(t, "revived", name)
		})
	}
}

func TestGrpcProxyRetentionAfterAccountColumnsDisabled(t *testing.T) {
	keyspace := os.Getenv("EVENT_LEDGER_TTL_TEST_KEYSPACE")
	if keyspace == "" {
		t.Skip("set EVENT_LEDGER_TTL_TEST_KEYSPACE to a prepared, isolated test keyspace")
	}
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.One
	cluster.Timeout = 5 * time.Second
	cluster.ConnectTimeout = 5 * time.Second
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: "cassandra", Password: "cassandra"}
	session, err := cluster.CreateSession()
	require.NoError(t, err)
	t.Cleanup(session.Close)

	handler := &CassandraHandler{session: session}
	handler.ConfigureV3Retention(config.V3Config{GrpcProxyEventTTLSeconds: 3, GrpcProxyStatsTTLSeconds: 3})
	ctx := logging.AttachLoggerToContext(context.Background(), otelzap.New(zap.NewNop()))
	ns := "grpc-proxy-account-rollback-" + uuid.NewString()
	now := time.Now().Truncate(time.Millisecond)
	for _, tt := range []struct {
		name  string
		table string
		write func(string, time.Time) error
	}{
		{"single event", "events_v3", func(context string, ts time.Time) error {
			return handler.UpsertEventV3(ctx, ns, context, "proxy", grpcProxySource, []byte(`{}`), ts)
		}},
		{"bulk event", "events_v3", func(context string, ts time.Time) error {
			return handler.BulkUpsertEventsV3(ctx, []data_access.EventV3UpsertRecord{{Namespace: ns, Context: context, EventName: "proxy", Source: grpcProxySource, Details: []byte(`{}`), Timestamp: ts}})
		}},
		{"single stats", statsV3Table, func(context string, ts time.Time) error {
			return handler.UpsertStatsV3(ctx, ns, context, "proxy", grpcProxySource, ts)
		}},
		{"bulk stats", statsV3Table, func(context string, ts time.Time) error {
			return handler.BulkUpsertStatsV3(ctx, []data_access.EventV3UpsertRecord{{Namespace: ns, Context: context, EventName: "proxy", Source: grpcProxySource, Timestamp: ts}})
		}},
		{"filtered stats", filteredStatsV3Table, func(context string, ts time.Time) error {
			return handler.UpsertFilteredStatsV3(ctx, ns, context, "proxy", grpcProxySource, ts, "", "")
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			context := uuid.NewString()
			where := " WHERE namespace = ? AND context = ?"
			if tt.table == "events_v3" {
				where += " AND event_name = 'proxy'"
			}
			t.Cleanup(func() { _ = session.Query("DELETE FROM "+tt.table+where, ns, context).Exec() })
			if tt.table == "events_v3" {
				require.NoError(t, session.Query("INSERT INTO events_v3 (namespace, context, event_name, source, details, timestamp, created_at, updated_at, nca_id, function_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ns, context, "proxy", grpcProxySource, []byte(`{}`), now, now, now, "account", "function").Exec())
			} else {
				require.NoError(t, session.Query("INSERT INTO "+tt.table+" (namespace, context, event_name, timestamp, created_at, updated_at, nca_id, function_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", ns, context, "legacy", now, now, now, "account", "function").Exec())
			}
			require.NoError(t, tt.write(context, now.Add(time.Second)))
			var ncaTTL, functionTTL int
			require.NoError(t, session.Query("SELECT TTL(nca_id), TTL(function_id) FROM "+tt.table+where, ns, context).Scan(&ncaTTL, &functionTTL))
			require.Greater(t, ncaTTL, 0)
			require.Greater(t, functionTTL, 0)
			time.Sleep(4 * time.Second)
			var name string
			require.ErrorIs(t, session.Query("SELECT event_name FROM "+tt.table+where, ns, context).Scan(&name), gocql.ErrNotFound)
		})
	}
}
