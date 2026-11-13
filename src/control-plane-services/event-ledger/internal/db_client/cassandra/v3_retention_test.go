package cassandra

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dsx-ai-factory/nvcf/src/control-plane-services/event-ledger/internal/config"
	"github.com/dsx-ai-factory/nvcf/src/control-plane-services/event-ledger/internal/data_access"
)

func TestGrpcProxyRetentionStatements(t *testing.T) {
	handler := &CassandraHandler{}
	handler.ConfigureV3Retention(config.V3Config{GrpcProxyEventTTLSeconds: 5, GrpcProxyStatsTTLSeconds: 10})
	now := time.Now()
	for _, tt := range []struct {
		source   string
		eventTTL int
		statsTTL int
	}{
		{grpcProxySource, 5, 10},
		{"nvidia-spot", 0, 0},
		{"grpc-proxy-other", 0, 0},
	} {
		ev := data_access.EventV3UpsertRecord{Namespace: "ns", Context: "c", EventName: "e", Source: tt.source, Timestamp: now}
		eventQuery, eventArgs := eventsV3InsertStatement(ev, now, now, handler.grpcProxyTTL(tt.source, false))
		statsQuery, statsArgs := statsV3InsertStatement(ev, now, now, handler.grpcProxyTTL(tt.source, true))
		if tt.eventTTL == 0 {
			require.NotContains(t, eventQuery, "USING TTL")
			require.Len(t, eventArgs, 8)
			require.NotContains(t, statsQuery, "USING TTL")
			require.Len(t, statsArgs, 6)
		} else {
			require.Contains(t, eventQuery, "USING TTL ?")
			require.Equal(t, tt.eventTTL, eventArgs[len(eventArgs)-1])
			require.Contains(t, statsQuery, "USING TTL ?")
			require.Equal(t, tt.statsTTL, statsArgs[len(statsArgs)-1])
		}
	}
	query, args := withTTL("INSERT INTO events_v3 (...) VALUES (...) IF NOT EXISTS", []any{"value"}, 5)
	require.Equal(t, "INSERT INTO events_v3 (...) VALUES (...) IF NOT EXISTS USING TTL ?", query)
	require.Equal(t, []any{"value", 5}, args)
}

func TestGrpcProxyStatsUpdateRefreshesRowMarker(t *testing.T) {
	for _, table := range []string{statsV3Table, filteredStatsV3Table} {
		t.Run(table, func(t *testing.T) {
			args := []any{"proxy", "ns", "ctx", "newer"}
			query, bound := statsV3UpdateStatement(table, "event_name = ?", "ns", "ctx", args, 5)
			require.Contains(t, query, "UPDATE "+table+" USING TTL ? SET event_name = ? WHERE namespace = ? AND context = ? IF timestamp < ?")
			require.Contains(t, query, "INSERT INTO "+table+" (namespace, context) VALUES (?, ?) USING TTL ?")
			require.Equal(t, []any{5, "proxy", "ns", "ctx", "newer", "ns", "ctx", 5}, bound)
			require.Equal(t, len(bound), strings.Count(query, "?"))

			query, bound = statsV3UpdateStatement(table, "event_name = ?", "ns", "ctx", args, 0)
			require.NotContains(t, query, "INSERT INTO")
			require.Equal(t, args, bound)
		})
	}
}
