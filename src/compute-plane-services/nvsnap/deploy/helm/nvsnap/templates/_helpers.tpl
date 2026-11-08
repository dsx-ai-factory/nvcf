{{/*
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
*/}}
{{/*
Chart name and a stable fullname. The "nvsnap.fullname" helper trims
release names so that "nvsnap-agent", "nvsnap-server" etc. remain readable.
*/}}
{{- define "nvsnap.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "nvsnap.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "nvsnap.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to every object the chart creates.
*/}}
{{- define "nvsnap.labels" -}}
helm.sh/chart: {{ include "nvsnap.chart" . }}
app.kubernetes.io/name: {{ include "nvsnap.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
{{- end }}

{{/*
Per-component labels and selector labels.

Components: agent, server, webhook.

Selector labels include only stable identity (name + instance + component);
they're embedded in Deployment/DaemonSet.spec.selector which is immutable
after first apply. Full labels include version + managed-by, which are
expected to change on upgrades — fine on metadata.labels, fatal on selector.
*/}}
{{- define "nvsnap.agent.selectorLabels" -}}
app: nvsnap-agent
app.kubernetes.io/name: nvsnap-agent
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: agent
{{- end }}

{{- define "nvsnap.agent.labels" -}}
{{ include "nvsnap.labels" . }}
app: nvsnap-agent
app.kubernetes.io/component: agent
{{- end }}

{{- define "nvsnap.server.selectorLabels" -}}
app: nvsnap-server
app.kubernetes.io/name: nvsnap-server
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: server
{{- end }}

{{- define "nvsnap.server.labels" -}}
{{ include "nvsnap.labels" . }}
app: nvsnap-server
app.kubernetes.io/component: server
{{- end }}



{{/*
Image references. Each helper resolves <registry>/<repository>:<tag>
where registry defaults to .Values.nvsnap.imageRegistry but can be
overridden per-component. Tag defaults to the chart's appVersion.

Why .nvsnap.* and not .global.* — Helm auto-merges `global:` into every
subchart's `.Values.global`, so a top-level `global.imageRegistry` of
`nvcr.io/...` leaks into Jaeger / cert-manager / gpu-operator and
causes them to try to pull their upstream images from our private
registry. Keeping our registry under our own namespace prevents that.
*/}}
{{- define "nvsnap.image" -}}
{{- $registry := .registry | default .ctx.Values.nvsnap.imageRegistry -}}
{{- /* Image tags are mandatory. We used to fall back to .Chart.AppVersion
       when .tag was empty, but that silently drifts every image that
       isn't explicitly --set (notably blobstore) to whatever Chart.yaml
       says, even if no such tag was ever built+pushed. Fail loudly
       instead so the install/upgrade can't render a known-broken
       reference. See nvsnap MR following the v0.0.5 blobstore
       ImagePullBackOff on GCP-H100-a (2026-06-02). */ -}}
{{- if not .tag -}}
{{- fail (printf "nvsnap.image: tag is required for repository %q (set %s.image.tag in values.yaml or --set)" .repository .repository) -}}
{{- end -}}
{{- printf "%s/%s:%s" $registry .repository .tag -}}
{{- end }}

{{- define "nvsnap.agent.image" -}}
{{- include "nvsnap.image" (dict "ctx" . "registry" .Values.agent.image.registry "repository" .Values.agent.image.repository "tag" .Values.agent.image.tag) -}}
{{- end }}

{{- define "nvsnap.server.image" -}}
{{- include "nvsnap.image" (dict "ctx" . "registry" .Values.server.image.registry "repository" .Values.server.image.repository "tag" .Values.server.image.tag) -}}
{{- end }}


{{- /* nvsnap.l2wait.image — image ref for the nvsnap-l2-wait init
       container (nvsnap#147). Uses the agent.l2.waitImage block;
       falls through to nvsnap.imageRegistry like every other NvSnap
       image. */ -}}
{{- define "nvsnap.l2wait.image" -}}
{{- include "nvsnap.image" (dict "ctx" . "registry" .Values.agent.l2.waitImage.registry "repository" .Values.agent.l2.waitImage.repository "tag" .Values.agent.l2.waitImage.tag) -}}
{{- end }}

{{- /* nvsnap.restoreBundle.image helper was retired in nvsnap#184.
       The webhook no longer injects a per-pod init container that
       pulls a nvsnap image; instead the nvsnap-agent DaemonSet itself
       stages the restore bundle to /var/lib/nvsnap/bundle via its
       nvsnap-bundle-stage initContainer, and the webhook injects
       hostPath mounts from there onto function pods. No
       cross-registry mirror required. */ -}}

{{/*
Image-pull-secrets block, rendered if any are configured. Used at the
PodSpec level (not Deployment-level — Kubernetes requires it on pods).
*/}}
{{- define "nvsnap.imagePullSecrets" -}}
{{- with .Values.nvsnap.imagePullSecrets }}
imagePullSecrets:
{{- range . }}
  - name: {{ . }}
{{- end }}
{{- end }}
{{- end }}

{{/*
nvsnap.server.internalLBError prints why server.service.annotations do not
make the load balancer internal on server.service.loadBalancerCloud, and
nothing when they do. The cloud must be named: an annotation of another
cloud is ignored where the Service lands, which then gets a public address,
so annotations of any other cloud are rejected too. At least one of the
named cloud's keys below must be present, and every one of them that is
present must have its internal value, exactly as the provider spells it: a
misspelled or contradicting annotation (GKE's legacy key "Internal" with the
current one "External", say) could leave the load balancer public.
*/}}
{{- define "nvsnap.server.internalLBError" -}}
{{- $a := .Values.server.service.annotations | default dict -}}
{{- $cloud := .Values.server.service.loadBalancerCloud | default "" | toString -}}
{{- $clouds := dict
  "aws" (dict
    "service.beta.kubernetes.io/aws-load-balancer-internal" "true"
    "service.beta.kubernetes.io/aws-load-balancer-scheme" "internal")
  "gcp" (dict
    "networking.gke.io/load-balancer-type" "Internal"
    "cloud.google.com/load-balancer-type" "Internal")
  "azure" (dict
    "service.beta.kubernetes.io/azure-load-balancer-internal" "true")
  "oci" (dict
    "service.beta.kubernetes.io/oci-load-balancer-internal" "true"
    "oci-network-load-balancer.oraclecloud.com/internal" "true") -}}
{{- if not (hasKey $clouds $cloud) -}}
server.service.loadBalancerCloud must be aws, gcp, azure or oci (got {{ $cloud | quote }}): the chart checks the internal annotation against that cloud
{{- else -}}
{{- $is := index $clouds $cloud -}}
{{- $seen := false -}}
{{- $bad := list -}}
{{- range $c, $keys := $clouds -}}
{{- range $k, $v := $keys -}}
{{- if hasKey $a $k -}}
{{- if ne $c $cloud -}}
{{- $bad = append $bad (printf "%s is an annotation of %s, not of %s" $k $c $cloud) -}}
{{- else if ne (toString (index $a $k)) $v -}}
{{- $bad = append $bad (printf "%s must be %q" $k $v) -}}
{{- else -}}
{{- $seen = true -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if $bad -}}
{{ join "; " $bad }}
{{- else if not $seen -}}
no annotation makes the {{ $cloud }} load balancer internal; set one of {{ keys $is | sortAlpha | join ", " }}
{{- end -}}
{{- end -}}
{{- end }}
