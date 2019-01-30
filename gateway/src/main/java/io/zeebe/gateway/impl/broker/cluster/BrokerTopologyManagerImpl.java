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

import static io.zeebe.gateway.impl.broker.BrokerClientImpl.LOG;

import com.esotericsoftware.minlog.Log;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.Member;
import io.zeebe.gateway.impl.broker.request.BrokerTopologyRequest;
import io.zeebe.protocol.impl.data.cluster.TopologyResponseDto;
import io.zeebe.raft.state.RaftState;
import io.zeebe.transport.ClientOutput;
import io.zeebe.transport.SocketAddress;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class BrokerTopologyManagerImpl extends Actor
    implements BrokerTopologyManager, ClusterMembershipEventListener {
  /** Interval in which the topology is refreshed even if the client is idle */
  public static final Duration MAX_REFRESH_INTERVAL_MILLIS = Duration.ofSeconds(10);

  /**
   * Shortest possible interval in which the topology is refreshed, even if the client is constantly
   * making new requests that require topology refresh
   */
  public static final Duration MIN_REFRESH_INTERVAL_MILLIS = Duration.ofMillis(300);

  protected final ClientOutput output;
  protected final BiConsumer<Integer, SocketAddress> registerEndpoint;

  protected final AtomicReference<BrokerClusterStateImpl> topology;
  // TODO: keep this implementation and remove the above
  protected final BrokerClusterStateImplAtomix newTopology;

  protected final List<CompletableActorFuture<BrokerClusterState>> nextTopologyFutures =
      new ArrayList<>();

  protected final BrokerTopologyRequest topologyRequest = new BrokerTopologyRequest();

  protected int refreshAttempt = 0;
  protected long lastRefreshTime = -1;
  private ObjectMapper objectMapper;

  public BrokerTopologyManagerImpl(
      final ClientOutput output, final BiConsumer<Integer, SocketAddress> registerEndpoint) {
    this.output = output;
    this.registerEndpoint = registerEndpoint;

    this.objectMapper = new ObjectMapper();
    this.topology = new AtomicReference<>(null);
    this.newTopology = new BrokerClusterStateImplAtomix();
  }

  public void setBrokers(Set<Member> brokers) {
    actor.call(
        () -> {
          for (Member broker : brokers) {
            if (broker.id().id().equals("gateway")) {
              continue;
            }

            try {
              int id = Integer.parseInt(broker.id().id());
              updatePartitions(broker.properties(), id);

              LOG.info("Broker {} has topology {}", id, broker.properties());
            } catch (NumberFormatException e) {
              LOG.info("Invalid broker id '{}'", broker.id().id());
            }
          }
        });
  }

  @Override
  protected void onActorStarted() {}

  public ActorFuture<Void> close() {
    return actor.close();
  }

  /** @return the current known cluster state or null if the topology was not fetched yet */
  @Override
  public BrokerClusterState getTopology() {
    return newTopology;
  }

  @Override
  public ActorFuture<BrokerClusterState> requestTopology() {
    final CompletableActorFuture<BrokerClusterState> future = new CompletableActorFuture<>();

    //    actor.run(
    //        () -> {
    //          final boolean isFirstStagedRequest = nextTopologyFutures.isEmpty();
    //          nextTopologyFutures.add(future);
    //
    //          if (isFirstStagedRequest) {
    //            scheduleNextRefresh();
    //          }
    //        });
    future.complete(newTopology);

    return future;
  }

  @Override
  public void provideTopology(final TopologyResponseDto topology) {
    //    actor.call(
    //        () -> {
    //          // TODO: not sure we should complete the refresh futures in this case,
    //          //   as the response could be older than the time when the future was submitted
    //          onNewTopology(topology);
    //        });
  }

  @Override
  public void event(ClusterMembershipEvent event) {
    actor.call(
        () -> {
          LOG.info("Gateway received event: {}", event);

          final Member node = event.subject();
          final Properties properties = node.properties();
          int brokerId;

          try {
            brokerId = Integer.parseInt(node.id().id());
          } catch (NumberFormatException e) {
            return;
          }

          switch (event.type()) {
            case METADATA_CHANGED:
            case MEMBER_ADDED:
              updatePartitions(properties, brokerId);

              break;

            case MEMBER_REMOVED:
              newTopology.removeBroker(brokerId);
              break;
          }
        });
  }

  private void updatePartitions(Properties properties, int brokerId) {
    for (String prop : properties.stringPropertyNames()) {
      if (prop.startsWith("partition-")
          && RaftState.valueOf(properties.getProperty(prop)) == RaftState.LEADER) {
        final int partitionId = Integer.parseInt(prop.split("-")[1]);

        newTopology.setPartitionLeader(partitionId, brokerId);
        newTopology.addPartitionIfAbsent(partitionId);

        //        if (newTopology.addBrokerIfAbsent(brokerId)) {
        newTopology.addBrokerIfAbsent(brokerId);
        final String clientApiAddress = properties.getProperty("clientAddress");
        addLeaderEndpoint(brokerId, clientApiAddress);
        //        }
        LOG.info("Added broker {} as leader of partition {}", brokerId, partitionId);
      }
    }
  }

  private void addLeaderEndpoint(int brokerId, String address) {
    try {
      final String socketAddress = objectMapper.readValue(address, String.class);
      registerEndpoint.accept(brokerId, SocketAddress.from(socketAddress));
    } catch (IOException e) {
      Log.error("Invalid client address for broker {}:  ", Integer.toString(brokerId), e);
    }
  }
}
