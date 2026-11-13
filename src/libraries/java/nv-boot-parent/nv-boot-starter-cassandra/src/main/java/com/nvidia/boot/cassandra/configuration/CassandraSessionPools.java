/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.boot.cassandra.configuration;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

// The connection pools of a session, read from the driver's pool manager.
//
// Node.getOpenConnections() also counts the control connection, so it cannot tell an empty pool
// from a working one. This reads each node's pool instead. That relies on driver internals, so
// the integration test checks it against a real driver.
//
// A node that the load balancing policy ignores has no pool by design. A node it uses that has
// no pool, for example while the driver is still creating it, makes the snapshot incomplete, and
// so does any failure to read the pools. problem says why; it is null when the snapshot is
// complete.
public record CassandraSessionPools(String problem, List<NodePool> nodes) {

    // One node as the session sees it. The pool counts are 0 when the node has no pool.
    // openConnections is the driver's count, which includes the control connection.
    public record NodePool(String endPoint, NodeState state, NodeDistance distance,
                           boolean pooled, boolean control, int openConnections, int poolSize,
                           int inFlight, int availableIds, int orphanedIds) {

        static NodePool of(Node node, ChannelPool pool, boolean control) {
            var pooled = pool != null;
            return new NodePool(node.getEndPoint().toString(), node.getState(), node.getDistance(),
                                pooled, control, node.getOpenConnections(),
                                pooled ? pool.size() : 0,
                                pooled ? pool.getInFlight() : 0,
                                pooled ? pool.getAvailableIds() : 0,
                                pooled ? pool.getOrphanedIds() : 0);
        }

        @Override
        public String toString() {
            return endPoint + " " + state + " " + distance
                   + (pooled ? " pool=" + poolSize + " inFlight=" + inFlight
                               + " free=" + availableIds + " orphaned=" + orphanedIds
                             : " no-pool")
                   + " open=" + openConnections + (control ? " control" : "");
        }
    }

    public static CassandraSessionPools of(CqlSession session) {
        try {
            // Read the context on every call: RefreshingCqlSession replaces its delegate session.
            if (!(session.getContext() instanceof InternalDriverContext context)) {
                return incomplete("the session does not expose the driver's internal context");
            }
            var pools = context.getPoolManager().getPools();
            var controlChannel = context.getControlConnection().channel();
            var controlEndPoint = controlChannel == null ? null : controlChannel.getEndPoint();
            var nodes = new ArrayList<NodePool>();
            String problem = null;
            for (var node : session.getMetadata().getNodes().values()) {
                var pool = pools.get(node);
                nodes.add(NodePool.of(node, pool, node.getEndPoint().equals(controlEndPoint)));
                if (pool == null && node.getDistance() != NodeDistance.IGNORED) {
                    problem = "a node the session uses has no pool";
                }
            }
            if (problem == null && nodes.stream().noneMatch(NodePool::pooled)) {
                problem = "no node has a pool";
            }
            return new CassandraSessionPools(problem, List.copyOf(nodes));
        } catch (RuntimeException e) {
            return incomplete("reading the pools failed: " + e);
        }
    }

    static CassandraSessionPools incomplete(String problem) {
        return new CassandraSessionPools(problem, List.of());
    }

    public boolean complete() {
        return problem == null;
    }

    // True only when the snapshot is complete and no pool has a connection, so missing data
    // never looks like a stuck session.
    public boolean allPoolsEmpty() {
        return complete()
               && nodes.stream().filter(NodePool::pooled).allMatch(node -> node.poolSize() == 0);
    }

    @Override
    public String toString() {
        var state = complete() ? "complete" : "incomplete (" + problem + ")";
        return nodes.isEmpty() ? state
                               : state + ": " + nodes.stream().map(NodePool::toString)
                                       .collect(Collectors.joining("; "));
    }
}
