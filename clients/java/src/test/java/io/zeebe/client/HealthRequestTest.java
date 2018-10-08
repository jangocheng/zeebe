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
package io.zeebe.client;

import static io.zeebe.client.util.RecordingGatewayService.broker;
import static io.zeebe.client.util.RecordingGatewayService.partition;
import static io.zeebe.gateway.protocol.GatewayOuterClass.Partition.PartitionBrokerRole.FOLLOW;
import static io.zeebe.gateway.protocol.GatewayOuterClass.Partition.PartitionBrokerRole.LEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import io.zeebe.client.api.commands.BrokerInfo;
import io.zeebe.client.api.commands.PartitionBrokerRole;
import io.zeebe.client.api.commands.PartitionInfo;
import io.zeebe.client.api.commands.Topology;
import io.zeebe.client.cmd.ClientException;
import io.zeebe.client.util.ClientTest;
import io.zeebe.gateway.protocol.GatewayOuterClass.HealthRequest;
import java.util.List;
import org.junit.Test;

public class HealthRequestTest extends ClientTest {

  @Test
  public void shouldRequestTopology() {
    // given
    gatewayService.onHealthRequest(
        broker("host1", 123, partition(0, LEADER), partition(1, FOLLOW)),
        broker("host2", 212, partition(0, FOLLOW), partition(1, LEADER)),
        broker("host3", 432, partition(0, FOLLOW), partition(1, FOLLOW)));

    // when
    final Topology topology = client.newTopologyRequest().send().join();

    // then
    final List<BrokerInfo> brokers = topology.getBrokers();
    assertThat(brokers).hasSize(3);

    BrokerInfo broker = brokers.get(0);
    assertThat(broker.getHost()).isEqualTo("host1");
    assertThat(broker.getPort()).isEqualTo(123);
    assertThat(broker.getAddress()).isEqualTo("host1:123");
    assertThat(broker.getPartitions())
        .extracting(PartitionInfo::getPartitionId, PartitionInfo::getRole)
        .containsOnly(tuple(0, PartitionBrokerRole.LEADER), tuple(1, PartitionBrokerRole.FOLLOWER));

    broker = brokers.get(1);
    assertThat(broker.getHost()).isEqualTo("host2");
    assertThat(broker.getPort()).isEqualTo(212);
    assertThat(broker.getAddress()).isEqualTo("host2:212");
    assertThat(broker.getPartitions())
        .extracting(PartitionInfo::getPartitionId, PartitionInfo::getRole)
        .containsOnly(tuple(0, PartitionBrokerRole.FOLLOWER), tuple(1, PartitionBrokerRole.LEADER));

    broker = brokers.get(2);
    assertThat(broker.getHost()).isEqualTo("host3");
    assertThat(broker.getPort()).isEqualTo(432);
    assertThat(broker.getAddress()).isEqualTo("host3:432");
    assertThat(broker.getPartitions())
        .extracting(PartitionInfo::getPartitionId, PartitionInfo::getRole)
        .containsOnly(
            tuple(0, PartitionBrokerRole.FOLLOWER), tuple(1, PartitionBrokerRole.FOLLOWER));
  }

  @Test
  public void shouldRaiseExceptionOnError() {
    // given
    gatewayService.errorOnRequest(
        HealthRequest.class, () -> new ClientException("Invalid request"));

    // when
    assertThatThrownBy(() -> client.newTopologyRequest().send().join())
        .isInstanceOf(ClientException.class)
        .hasMessageContaining("Invalid request");
  }
}
