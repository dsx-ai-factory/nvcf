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

package function

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/icms-translate/translate/common"
)

const pylonDrainTimeoutEnv = "PYLON_SHUTDOWN_DRAIN_TIMEOUT_MS"

func llmContainerFunctionEnv() map[string]string {
	return map[string]string{
		common.ContainerFunctionImageEnv: "nvcr.io/nvidia/function:latest",
		common.UtilsImageEnv:             "nvcr.io/nvidia/utils:latest",
		common.InitImageEnv:              "nvcr.io/nvidia/init:latest",
		"LLM_REQUEST_ROUTER_ADDRESS":     "llm-router.example.com:443",
		"INFERENCE_PORT":                 "8080",
		"NVCF_WORKER_TOKEN":              "worker-token",
		"NVCF_FQDN_GRPC":                 "grpc.example.com",
		"FUNCTION_ID":                    "function-id",
		"FUNCTION_VERSION_ID":            "function-version-id",
		"NCA_ID":                         "nca-id",
	}
}

// translateLLMContainerPod translates an LLM container function with extra
// launch environment and the given inference stop pause (zero for default).
func translateLLMContainerPod(t *testing.T, extraEnv map[string]string, pause time.Duration) *corev1.Pod {
	t.Helper()

	tcfg := TranslateConfig{TranslateConfig: newFunctionTranslateConfig(nil), LLMInferenceStopPause: metav1.Duration{Duration: pause}}
	return translateLLMContainerPodWithConfig(t, extraEnv, tcfg)
}

// translateLLMContainerPodWithConfig translates an LLM container function with
// extra launch environment and the given translate config.
func translateLLMContainerPodWithConfig(t *testing.T, extraEnv map[string]string, tcfg TranslateConfig) *corev1.Pod {
	t.Helper()

	env := llmContainerFunctionEnv()
	for k, v := range extraEnv {
		env[k] = v
	}

	msg := newContainerFunctionMessage()
	msg.Details.FunctionType = FunctionTypeLLM
	msg.LaunchSpecification.EnvironmentB64 = encodeTextEnv(env)

	objs, err := Translate(msg, tcfg)
	require.NoError(t, err)
	return findPodByName(t, objs, "0-request")
}

// The icms-translate CLI decodes its config file with encoding/json and
// unknown fields disallowed; the pause must read as a duration string there.
func TestTranslateConfigJSONStopPause(t *testing.T) {
	for _, tc := range []struct {
		name      string
		json      string
		wantPause time.Duration
		wantErr   bool
	}{
		{name: "duration string", json: `{"llmInferenceStopPause":"90s"}`, wantPause: 90 * time.Second},
		{name: "absent keeps the default", json: `{}`, wantPause: 60 * time.Second},
		{name: "unitless number is rejected", json: `{"llmInferenceStopPause":90}`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tcfg := TranslateConfig{TranslateConfig: newFunctionTranslateConfig(nil)}
			dec := json.NewDecoder(strings.NewReader(tc.json))
			dec.DisallowUnknownFields()
			err := dec.Decode(&tcfg)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			pod := translateLLMContainerPodWithConfig(t, nil, tcfg)
			assert.Equal(t, tc.wantPause, preStopSleep(t, findContainerByName(t, pod.Spec.Containers, inferenceContainerName)))
		})
	}
}

// preStopSleep returns how long the container's preStop hook pauses, or fails
// the test when the container has no exec sleep preStop hook.
func preStopSleep(t *testing.T, c corev1.Container) time.Duration {
	t.Helper()

	require.NotNil(t, c.Lifecycle, "container %q has no lifecycle hooks", c.Name)
	require.NotNil(t, c.Lifecycle.PreStop, "container %q has no preStop hook", c.Name)
	require.NotNil(t, c.Lifecycle.PreStop.Exec, "container %q preStop is not an exec hook", c.Name)
	command := c.Lifecycle.PreStop.Exec.Command
	require.Len(t, command, 2, "preStop command %v", command)
	require.Equal(t, "sleep", command[0])
	seconds, err := strconv.Atoi(command[1])
	require.NoError(t, err)
	return time.Duration(seconds) * time.Second
}

// pylonDrainTimeout returns the drain deadline the Pylon container is given, or
// fails the test when the container does not set one.
func pylonDrainTimeout(t *testing.T, c corev1.Container) time.Duration {
	t.Helper()

	value, ok := envSliceToMap(c.Env)[pylonDrainTimeoutEnv]
	require.True(t, ok, "container %q has no %s", c.Name, pylonDrainTimeoutEnv)
	ms, err := strconv.Atoi(value)
	require.NoError(t, err)
	return time.Duration(ms) * time.Millisecond
}

func gracePeriod(t *testing.T, pod *corev1.Pod) time.Duration {
	t.Helper()

	require.NotNil(t, pod.Spec.TerminationGracePeriodSeconds)
	return time.Duration(*pod.Spec.TerminationGracePeriodSeconds) * time.Second
}

func TestTranslateContainerLLMShutdownFollowsStopPause(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		configured                   time.Duration
		wantPause, wantDrain, wantGP time.Duration
	}{
		{name: "default", configured: 0, wantPause: 60 * time.Second, wantDrain: 55 * time.Second, wantGP: 120 * time.Second},
		{name: "shorter pause keeps the default grace period", configured: 45 * time.Second,
			wantPause: 45 * time.Second, wantDrain: 40 * time.Second, wantGP: 120 * time.Second},
		{name: "longer pause raises the grace period", configured: 90 * time.Second,
			wantPause: 90 * time.Second, wantDrain: 85 * time.Second, wantGP: 150 * time.Second},
		{name: "partial seconds round down", configured: 90*time.Second + 700*time.Millisecond,
			wantPause: 90 * time.Second, wantDrain: 85 * time.Second, wantGP: 150 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := translateLLMContainerPod(t, nil, tc.configured)

			inference := findContainerByName(t, pod.Spec.Containers, inferenceContainerName)
			worker := findContainerByName(t, pod.Spec.Containers, LLMWorkerContainerName)
			assert.Equal(t, tc.wantPause, preStopSleep(t, inference))
			assert.Equal(t, tc.wantDrain, pylonDrainTimeout(t, worker))
			assert.Equal(t, tc.wantGP, gracePeriod(t, pod))
		})
	}
}

func TestTranslateContainerLLMShutdownFitsGracePeriod(t *testing.T) {
	for _, configured := range []time.Duration{
		0, 10 * time.Second, 60 * time.Second, 5 * time.Minute,
		time.Duration(math.MaxInt64), // the grace arithmetic must not overflow
	} {
		t.Run(configured.String(), func(t *testing.T) {
			pod := translateLLMContainerPod(t, nil, configured)

			pause := preStopSleep(t, findContainerByName(t, pod.Spec.Containers, inferenceContainerName))
			drain := pylonDrainTimeout(t, findContainerByName(t, pod.Spec.Containers, LLMWorkerContainerName))
			require.NotNil(t, pod.Spec.TerminationGracePeriodSeconds)

			assert.Positive(t, drain)
			assert.GreaterOrEqual(t, pause, drain+5*time.Second,
				"vLLM must keep serving until Pylon's drain and its 5s shutdown are over")
			// Whole seconds, so the check itself cannot overflow.
			assert.GreaterOrEqual(t, *pod.Spec.TerminationGracePeriodSeconds, int64(pause/time.Second)+60,
				"the grace period must leave vLLM 60s to exit after the pause")
		})
	}
}

func TestTranslateContainerLLMWorkerDrainTimeoutOverridesFunctionEnv(t *testing.T) {
	pod := translateLLMContainerPod(t, map[string]string{pylonDrainTimeoutEnv: "90000"}, 0)

	worker := findContainerByName(t, pod.Spec.Containers, LLMWorkerContainerName)
	var values []string
	for _, env := range worker.Env {
		if env.Name == pylonDrainTimeoutEnv {
			values = append(values, env.Value)
		}
	}
	assert.Equal(t, []string{"55000"}, values,
		"a function's own value must not outlast the inference pause")
}

func TestTranslateContainerNonLLMKeepsDefaultShutdown(t *testing.T) {
	msg := newContainerFunctionMessage()
	msg.LaunchSpecification.EnvironmentB64 = encodeTextEnv(map[string]string{
		common.ContainerFunctionImageEnv: "nvcr.io/nvidia/function:latest",
		common.UtilsImageEnv:             "nvcr.io/nvidia/utils:latest",
		common.InitImageEnv:              "nvcr.io/nvidia/init:latest",
	})
	tcfg := TranslateConfig{TranslateConfig: newFunctionTranslateConfig(nil), LLMInferenceStopPause: metav1.Duration{Duration: 5 * time.Minute}}

	objs, err := Translate(msg, tcfg)
	require.NoError(t, err)

	pod := findPodByName(t, objs, "0-request")
	inference := findContainerByName(t, pod.Spec.Containers, inferenceContainerName)
	assert.Nil(t, inference.Lifecycle)
	assert.Equal(t, 120*time.Second, gracePeriod(t, pod))
}

func TestTranslateHelmChartLLMWorkerHasNoDrainTimeout(t *testing.T) {
	msg := newHelmFunctionMessage()
	msg.Details.FunctionType = FunctionTypeLLM
	msg.LaunchSpecification.EnvironmentB64 = encodeTextEnv(map[string]string{
		common.UtilsImageEnv:                "utils:latest",
		common.InitImageEnv:                 "init:latest",
		"LLM_REQUEST_ROUTER_ADDRESS":        "llm-router.example.com:443",
		"INFERENCE_PORT":                    "8080",
		"HELM_CHART_INFERENCE_SERVICE_NAME": "inference-svc",
		"NVCF_WORKER_TOKEN":                 "worker-token",
		"NVCF_FQDN_GRPC":                    "grpc.example.com",
		"FUNCTION_ID":                       "function-id",
		"FUNCTION_VERSION_ID":               "function-version-id",
		"NCA_ID":                            "nca-id",
	})

	objs, err := Translate(msg, TranslateConfig{TranslateConfig: newFunctionTranslateConfig(nil)})
	require.NoError(t, err)

	worker := findContainerByName(t, findPodByName(t, objs, common.UtilsPodName).Spec.Containers, LLMWorkerContainerName)
	assert.NotContains(t, envSliceToMap(worker.Env), pylonDrainTimeoutEnv)
}
