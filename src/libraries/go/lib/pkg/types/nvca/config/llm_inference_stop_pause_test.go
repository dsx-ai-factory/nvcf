/*
SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

package nvcaconfig

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeConfigLLMInferenceStopPause(t *testing.T) {
	for _, tc := range []struct {
		name    string
		yaml    string
		want    time.Duration
		wantErr string
	}{
		{name: "unset keeps the translator default", yaml: "workload: {}\n", want: 0},
		{name: "operator value", yaml: "workload:\n  llmInferenceStopPause: 90s\n", want: 90 * time.Second},
		{name: "minimum", yaml: "workload:\n  llmInferenceStopPause: 10s\n", want: 10 * time.Second},
		{name: "maximum", yaml: "workload:\n  llmInferenceStopPause: 1h\n", want: time.Hour},
		{
			name:    "too short to leave Pylon a drain",
			yaml:    "workload:\n  llmInferenceStopPause: 5s\n",
			wantErr: "workload.llmInferenceStopPause must be between 10s and 1h0m0s",
		},
		{
			name:    "negative",
			yaml:    "workload:\n  llmInferenceStopPause: -30s\n",
			wantErr: "workload.llmInferenceStopPause must be between 10s and 1h0m0s",
		},
		{
			name:    "longer than any useful drain",
			yaml:    "workload:\n  llmInferenceStopPause: 2h\n",
			wantErr: "workload.llmInferenceStopPause must be between 10s and 1h0m0s",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := DecodeConfig([]byte(tc.yaml))
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, cfg.Workload.LLMInferenceStopPause)
		})
	}
}

// The nvca-operator chart's agentConfig.mergeConfig reaches the agent as an
// extra config document merged over the generated one.
func TestDecodeConfigLLMInferenceStopPauseFromMergedConfig(t *testing.T) {
	base := []byte("workload:\n  stargateQUICInsecure: true\n")
	merge := []byte("workload:\n  llmInferenceStopPause: 2m\n")

	cfg, err := DecodeConfig(base, merge)

	require.NoError(t, err)
	assert.Equal(t, 2*time.Minute, cfg.Workload.LLMInferenceStopPause)
	assert.True(t, cfg.Workload.StargateQUICInsecure)
}
