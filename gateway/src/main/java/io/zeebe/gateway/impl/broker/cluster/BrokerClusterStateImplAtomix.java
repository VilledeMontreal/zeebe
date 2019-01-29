/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.gateway.impl.broker.cluster;

import static io.zeebe.transport.ClientTransport.UNKNOWN_NODE_ID;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.IntArrayList;

public class BrokerClusterStateImplAtomix implements BrokerClusterState {

  private final Int2IntHashMap partitionLeaders = new Int2IntHashMap(NODE_ID_NULL);
  private final IntArrayList brokers = new IntArrayList(5, NODE_ID_NULL);
  private final IntArrayList partitions = new IntArrayList(32, PARTITION_ID_NULL);
  private final Random randomBroker = new Random();
  private int clusterSize;
  private int partitionsCount;
  private int replicationFactor;

  public void setReplicationFactor(int replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  public void setPartitionLeader(int partitionId, int leaderId) {
    partitionLeaders.put(partitionId, leaderId);
  }

  public void addPartitionIfAbsent(int partitionId) {
    if (partitions.indexOf(partitionId) == -1) {
      partitions.addInt(partitionId);
      partitionsCount++;
    }
  }

  /** return true if added a broker */
  public boolean addBrokerIfAbsent(int nodeId) {
    if (brokers.indexOf(nodeId) == -1) {
      brokers.addInt(nodeId);
      ++clusterSize;
      return true;
    }
    return false;
  }

  public void removeBroker(int brokerId) {
    brokers.removeInt(brokerId);
    clusterSize--;
    partitions.forEach(
        key -> {
          if (partitionLeaders.get(key) == null) {
            partitionLeaders.remove(key);
          }
        });
  }

  @Override
  public int getClusterSize() {
    return clusterSize;
  }

  @Override
  public int getPartitionsCount() {
    return getPartitions().size();
  }

  @Override
  public int getLeaderForPartition(int partition) {
    return partitionLeaders.get(partition);
  }

  @Override
  public int getRandomBroker() {
    if (brokers.isEmpty()) {
      return UNKNOWN_NODE_ID;
    } else {
      return randomBroker.nextInt(brokers.size());
    }
  }

  @Override
  public List<Integer> getPartitions() {
    // TODO: see if this can be eliminated
    return partitionLeaders.keySet().stream().collect(Collectors.toList());
  }

  @Override
  public int getPartition(int offset) {
    return partitions.get(offset);
  }

  @Override
  public String toString() {
    return "BrokerClusterStateImpl{"
        + "partitionLeaders="
        + partitionLeaders
        + ", brokers="
        + brokers
        + ", partitions="
        + partitions
        + ", clusterSize="
        + clusterSize
        + ", partitionsCount="
        + partitionsCount
        + ", replicationFactor="
        + replicationFactor
        + '}';
  }
}
