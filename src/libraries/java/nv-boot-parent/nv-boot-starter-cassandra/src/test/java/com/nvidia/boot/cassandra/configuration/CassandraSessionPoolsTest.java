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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.session.PoolManager;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CassandraSessionPoolsTest {

    private final InternalDriverContext context = mock(InternalDriverContext.class);
    private final PoolManager poolManager = mock(PoolManager.class);
    private final ControlConnection controlConnection = mock(ControlConnection.class);
    private final Map<UUID, Node> nodes = new LinkedHashMap<>();
    private final Map<Node, ChannelPool> pools = new LinkedHashMap<>();

    @Test
    void reportsPoolSizeSeparatelyFromTheControlConnection() {
        var node = node("10.0.0.1", NodeDistance.LOCAL, 2);
        pools.put(node, pool(1, 3, 1021));
        controlOn(node);

        var snapshot = CassandraSessionPools.of(session());

        assertThat(snapshot.complete()).isTrue();
        assertThat(snapshot.nodes()).singleElement().satisfies(entry -> {
            assertThat(entry.pooled()).isTrue();
            assertThat(entry.control()).isTrue();
            assertThat(entry.openConnections()).isEqualTo(2);
            assertThat(entry.poolSize()).isEqualTo(1);
            assertThat(entry.inFlight()).isEqualTo(3);
            assertThat(entry.availableIds()).isEqualTo(1021);
        });
    }

    // The case the driver's open count hides: the control connection is up, the pool is empty.
    @Test
    void reportsAnEmptyPoolWhileTheControlConnectionIsOpen() {
        var node = node("10.0.0.1", NodeDistance.LOCAL, 1);
        pools.put(node, pool(0, 0, 0));
        controlOn(node);

        var snapshot = CassandraSessionPools.of(session());

        assertThat(snapshot.complete()).isTrue();
        assertThat(snapshot.nodes()).singleElement().satisfies(entry -> {
            assertThat(entry.openConnections()).isEqualTo(1);
            assertThat(entry.poolSize()).isZero();
        });
    }

    @Test
    void usedNodeWithoutAPoolMakesTheSnapshotIncomplete() {
        pools.put(node("10.0.0.1", NodeDistance.LOCAL, 1), pool(1, 0, 1024));
        node("10.0.0.2", NodeDistance.LOCAL, 0);

        var snapshot = CassandraSessionPools.of(session());

        assertThat(snapshot.complete()).isFalse();
        assertThat(snapshot.problem()).isEqualTo("a node the session uses has no pool");
        assertThat(snapshot.nodes()).hasSize(2);
    }

    @Test
    void ignoredNodeWithoutAPoolIsExpected() {
        pools.put(node("10.0.0.1", NodeDistance.LOCAL, 1), pool(1, 0, 1024));
        node("10.1.0.1", NodeDistance.IGNORED, 0);

        var snapshot = CassandraSessionPools.of(session());

        assertThat(snapshot.complete()).isTrue();
        assertThat(snapshot.nodes()).filteredOn(CassandraSessionPools.NodePool::pooled).hasSize(1);
    }

    @Test
    void allPoolsEmptyWhenNoPoolHasAConnection() {
        pools.put(node("10.0.0.1", NodeDistance.LOCAL, 1), pool(0, 0, 0));
        pools.put(node("10.0.0.2", NodeDistance.LOCAL, 0), pool(0, 0, 0));
        node("10.1.0.1", NodeDistance.IGNORED, 0);

        assertThat(CassandraSessionPools.of(session()).allPoolsEmpty()).isTrue();
    }

    @Test
    void oneConnectedPoolIsNotEmpty() {
        pools.put(node("10.0.0.1", NodeDistance.LOCAL, 0), pool(0, 0, 0));
        pools.put(node("10.0.0.2", NodeDistance.LOCAL, 1), pool(1, 1024, 0));

        assertThat(CassandraSessionPools.of(session()).allPoolsEmpty()).isFalse();
    }

    // A pool that is missing, for example while the driver creates it, is not proof of an empty
    // session.
    @Test
    void incompleteSnapshotIsNeverEmpty() {
        pools.put(node("10.0.0.1", NodeDistance.LOCAL, 0), pool(0, 0, 0));
        node("10.0.0.2", NodeDistance.LOCAL, 0);

        assertThat(CassandraSessionPools.of(session()).allPoolsEmpty()).isFalse();
        assertThat(CassandraSessionPools.incomplete("unreadable").allPoolsEmpty()).isFalse();
    }

    @Test
    void noPoolsAtAllIsIncomplete() {
        node("10.1.0.1", NodeDistance.IGNORED, 0);

        assertThat(CassandraSessionPools.of(session()).problem()).isEqualTo("no node has a pool");
    }

    @Test
    void noNodesIsIncomplete() {
        assertThat(CassandraSessionPools.of(session()).complete()).isFalse();
    }

    @Test
    void sessionWithoutTheInternalContextIsIncomplete() {
        var session = mock(CqlSession.class);
        when(session.getContext()).thenReturn(mock(DriverContext.class));

        var snapshot = CassandraSessionPools.of(session);

        assertThat(snapshot.complete()).isFalse();
        assertThat(snapshot.nodes()).isEmpty();
    }

    @Test
    void failureToReadThePoolsIsIncomplete() {
        var session = session();
        when(poolManager.getPools()).thenThrow(new IllegalStateException("closed"));

        assertThat(CassandraSessionPools.of(session).problem())
                .startsWith("reading the pools failed");
    }

    private CqlSession session() {
        var session = mock(CqlSession.class);
        var metadata = mock(Metadata.class);
        when(session.getContext()).thenReturn(context);
        when(session.getMetadata()).thenReturn(metadata);
        when(metadata.getNodes()).thenReturn(nodes);
        when(context.getPoolManager()).thenReturn(poolManager);
        when(context.getControlConnection()).thenReturn(controlConnection);
        when(poolManager.getPools()).thenReturn(pools);
        return session;
    }

    private Node node(String address, NodeDistance distance, int openConnections) {
        var node = mock(Node.class);
        when(node.getEndPoint())
                .thenReturn(new DefaultEndPoint(new InetSocketAddress(address, 9042)));
        when(node.getState()).thenReturn(NodeState.UP);
        when(node.getDistance()).thenReturn(distance);
        when(node.getOpenConnections()).thenReturn(openConnections);
        nodes.put(UUID.randomUUID(), node);
        return node;
    }

    private static ChannelPool pool(int size, int inFlight, int availableIds) {
        var pool = mock(ChannelPool.class);
        when(pool.size()).thenReturn(size);
        when(pool.getInFlight()).thenReturn(inFlight);
        when(pool.getAvailableIds()).thenReturn(availableIds);
        return pool;
    }

    private void controlOn(Node node) {
        var endPoint = node.getEndPoint();
        var channel = mock(DriverChannel.class);
        when(channel.getEndPoint()).thenReturn(endPoint);
        when(controlConnection.channel()).thenReturn(channel);
    }
}
