package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestV3RetentionConfig(t *testing.T) {
	defaults := (V3Config{}).WithDefaults()
	require.Equal(t, 10800, defaults.GrpcProxyEventTTLSeconds)
	require.Equal(t, 259200, defaults.GrpcProxyStatsTTLSeconds)
	require.NoError(t, defaults.Validate())

	override := V3Config{GrpcProxyEventTTLSeconds: 5, GrpcProxyStatsTTLSeconds: 10}.WithDefaults()
	require.Equal(t, 5, override.GrpcProxyEventTTLSeconds)
	require.Equal(t, 10, override.GrpcProxyStatsTTLSeconds)
	require.NoError(t, override.Validate())

	for _, tt := range []struct {
		name string
		cfg  V3Config
	}{
		{"zero event", V3Config{GrpcProxyStatsTTLSeconds: 1}},
		{"negative event", V3Config{GrpcProxyEventTTLSeconds: -1, GrpcProxyStatsTTLSeconds: 1}},
		{"too large event", V3Config{GrpcProxyEventTTLSeconds: MaxCassandraTTLSeconds + 1, GrpcProxyStatsTTLSeconds: 1}},
		{"zero stats", V3Config{GrpcProxyEventTTLSeconds: 1}},
		{"negative stats", V3Config{GrpcProxyEventTTLSeconds: 1, GrpcProxyStatsTTLSeconds: -1}},
		{"too large stats", V3Config{GrpcProxyEventTTLSeconds: 1, GrpcProxyStatsTTLSeconds: MaxCassandraTTLSeconds + 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, tt.cfg.Validate())
		})
	}
}
