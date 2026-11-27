#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

context=$1 namespace=$2 pod=$3 action=$4
case "$action" in pause|resume) ;; *) exit 2 ;; esac
# Process variables are expanded inside the sidecar.
# shellcheck disable=SC2016
kubectl --context "$context" -n "$namespace" exec "$pod" -c auto-unseal-sidecar -- \
    sh -ec '
    action=$1
    for attempt in 1 2 3 4 5; do
        for file in /proc/[0-9]*/comm; do
            IFS= read -r name < "$file" || continue
            test "$name" = sleep || continue
            pid=${file%/comm}
            pid=${pid##*/}
            ppid=$(awk '\''$1 == "PPid:" { print $2 }'\'' "/proc/$pid/status" 2>/dev/null) || continue
            test "$ppid" = 1 || continue
            state=$(awk '\''$1 == "State:" { print $2 }'\'' "/proc/$pid/status" 2>/dev/null) || continue
            if test "$action" = resume && test "$state" != T; then
                continue
            fi
            if test "$action" = pause; then
                kill -STOP "$pid" 2>/dev/null || continue
                stopped=false
                for check in 1 2 3 4 5; do
                    state=$(awk '\''$1 == "State:" { print $2 }'\'' "/proc/$pid/status" 2>/dev/null) || break
                    if test "$state" = T; then stopped=true; break; fi
                    sleep 0.1
                done
                if test "$stopped" != true; then
                    kill -CONT "$pid" 2>/dev/null || true
                    continue
                fi
                # Recover even when a later scenario assertion fails.
                (sleep 240; kill -CONT "$pid" 2>/dev/null || true) </dev/null >/dev/null 2>&1 &
            else
                kill -CONT "$pid" 2>/dev/null || continue
            fi
            printf "monitor=%s\n" "$action"
            exit 0
        done
        if test "$action" = resume; then
            printf "monitor=resume\n"
            exit 0
        fi
        sleep 1
    done
    echo "Unseal monitor sleep process not found" >&2
    exit 1
    ' qa-monitor "$action"
