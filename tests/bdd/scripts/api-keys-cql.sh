#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Run one CQL statement against the self-managed Cassandra as its admin user
# and print the result. The admin password comes from the cassandra Helm
# release values and goes to cqlsh over stdin, so it never appears in this
# script's argv or in the BDD command logs.
#
# Usage: api-keys-cql.sh <kubeconfig> <kube-context> <cql>
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <kubeconfig> <kube-context> <cql>" >&2
  exit 2
fi
kubeconfig=$1
context=$2
cql=$3

helm --kubeconfig "${kubeconfig}" --kube-context "${context}" -n cassandra-system \
  get values cassandra -a -o json | jq -r .cassandra.dbUser.password |
  kubectl --kubeconfig "${kubeconfig}" --context "${context}" -n cassandra-system \
    exec -i cassandra-0 -c cassandra -- \
    sh -c '/opt/cassandra/bin/cqlsh -u cassandra -p "$(cat)" localhost -e "$0"' "${cql}"
