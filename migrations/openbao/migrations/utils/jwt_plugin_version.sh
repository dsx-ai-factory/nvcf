#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# An empty result preserves the published legacy migration behavior. Versioned
# deployments mount the manifest copied from their actual server image.
jwt_plugin_version() {
    local version=${BAO_JWT_PLUGIN_VERSION:-}
    local manifest_values manifest_target bridge_image
    if [[ -n ${BAO_JWT_PLUGIN_VERSIONS_FILE:-} ]]; then
        [[ -r $BAO_JWT_PLUGIN_VERSIONS_FILE ]] || {
            echo 'JWT plugin version manifest is missing' >&2
            return 1
        }
        manifest_values=$(
            unset JWT_PLUGIN_TARGET_VERSION JWT_PLUGIN_BRIDGE_IMAGE
            # shellcheck source=infra/openbao/jwt-plugin-versions.env
            source "$BAO_JWT_PLUGIN_VERSIONS_FILE" || exit 1
            printf '%s|%s' "${JWT_PLUGIN_TARGET_VERSION:-}" "${JWT_PLUGIN_BRIDGE_IMAGE:-}"
        ) || return 1
        manifest_target=${manifest_values%%|*}
        bridge_image=${manifest_values#*|}
        [[ -n $manifest_target ]] || { echo 'JWT target version is missing' >&2; return 1; }
        if [[ -n $bridge_image && -n $version && $version != "$manifest_target" ]]; then
            echo 'Target-only migrations must select the manifest target; remove the old-version override' >&2
            return 1
        fi
        version=${version:-$manifest_target}
    fi
    if [[ -z ${BAO_JWT_PLUGIN_VERSIONS_FILE:-} && ${JWT_PLUGIN_LIFECYCLE_MODE:-legacy} == versioned ]]; then
        echo 'Versioned JWT mounts require the server image version manifest' >&2
        return 1
    fi
    if [[ -n $version && ! $version =~ ^v[0-9]+\.[0-9]+\.[0-9]+([+-][0-9A-Za-z.-]+)?$ ]]; then
        echo 'Invalid JWT plugin version' >&2
        return 1
    fi
    printf '%s' "$version"
}
