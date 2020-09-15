/*
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
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
package io.atomix.raft;

import static org.assertj.core.api.Assertions.assertThat;

import io.atomix.cluster.MemberId;
import io.atomix.raft.RaftServer.Role;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class RaftControlledTest {

  @Rule public RaftContextRule raftRule = new RaftContextRule(3);

  private List<Runnable> operations = new ArrayList();

  @Before
  public void init() {
    operations = new ArrayList<>();
    final var serverIds = raftRule.getRaftServers().keySet();
    serverIds.forEach(
        memberId -> {
          operations.add(() -> raftRule.runUntilDone(memberId));
          operations.add(() -> raftRule.runNextTask(memberId));
          operations.add(() -> raftRule.processAllMessage(memberId));
          operations.add(() -> raftRule.processNextMessage(memberId));
          operations.add(() -> raftRule.tickHeartbeatTimeout(memberId));
          operations.add(() -> raftRule.tickElectionTimeout(memberId));
          operations.add(() -> raftRule.clientAppend(memberId));
          serverIds.forEach(
              other -> {
                if (other != memberId) {
                  operations.add(() -> raftRule.getServerProtocol(memberId).deliverAll(other));
                  operations.add(
                      () -> raftRule.getServerProtocol(memberId).deliverNextMessage(other));
                  // operations.add(() ->
                  // raftRule.getServerProtocol(memberId).dropNextMessage(other));
                }
              });
        });
    operations.add(() -> raftRule.clientAppendOnLeader());
    serverIds.forEach(s -> raftRule.getServerProtocol(s).setDeliverImmediately(false));
  }

  @Test
  public void shouldJoin() {
    assertThat(raftRule.getRaftServer(0).getRaftRole().role() == Role.LEADER);
  }

  @Test
  public void shouldBackToBackLeaderElection() {
    assertThat(raftRule.getRaftServer(0).getRaftRole().role() == Role.LEADER);
    raftRule.clientAppend(MemberId.from(String.valueOf(0)));
    raftRule.runUntilDone();
    raftRule.tickHeartbeatTimeout(0);
    raftRule.runUntilDone();
    raftRule.tickElectionTimeout(1);
    raftRule.tickElectionTimeout(1);
    raftRule.runUntilDone(); // accepts poll requests
    raftRule.runUntilDone(1);
    raftRule.runUntilDone(0); // accepts vote
    raftRule.runUntilDone(2); // 2 accepts vote
    raftRule.tickElectionTimeout(2);
    raftRule.tickElectionTimeout(2); // 2 starts new election
    raftRule.runUntilDone(0); // accepts poll
    raftRule.getServerProtocol(1).setDeliverImmediately(false);
    raftRule.runUntilDone(1); // 1 receives vote response and become leader at term 2
    raftRule.tickHeartbeatTimeout(1); // commit
    raftRule.runUntilDone(2); // send vote
    raftRule.getServerProtocol(1).deliverAll(MemberId.from(String.valueOf(0)));
    raftRule.runUntilDone(0); // accept vote
    raftRule.runUntilDone(2);
    raftRule.runUntilDone(0);
    raftRule.runUntilDone(1);
    raftRule.runUntilDone();
  }

  @Test
  public void randomizedTest() {
    final var random = new Random(100);
    for (int i = 0; i < 100000; i++) {
      final var nextOp = Math.abs(random.nextInt());

      operations.get(nextOp % operations.size()).run();
      raftRule.assertOnlyOneLeader();
      // raftRule.assertAllLogsEqual();
    }

    raftRule.assertAllLogsEqual();
  }

  private void addWithProbability(
      final List<Pair<RaftOperation, Double>> pmf,
      final Consumer<MemberId> operation,
      final Double probability,
      final String operationName) {
    pmf.add(new Pair<>(new RaftOperation(operationName, operation), probability));
  }

  @Test
  public void randomizedTestWithEnumeration() {

    final List<Pair<RaftOperation, Double>> pmf = new ArrayList<>();
    final var serverIds = raftRule.getRaftServers().keySet().stream().collect(Collectors.toList());
    /*serverIds.forEach(
    memberId -> {
      addWithProbability(
          pmf, () -> raftRule.runUntilDone(memberId), 0.3, "runUntildone", memberId);
      addWithProbability(
          pmf, () -> raftRule.runNextTask(memberId), 0.1, "runNextTask", memberId);
      addWithProbability(
          pmf, () -> raftRule.processAllMessage(memberId), 0.3, "processAllMessage", memberId);
      addWithProbability(
          pmf, () -> raftRule.processNextMessage(memberId), 0.05, "processNextMessage", memberId);
      addWithProbability(
          pmf, () -> raftRule.tickHeartbeatTimeout(memberId), 0.1, "tickHeartBeat", memberId);
      addWithProbability(
          pmf,
          () -> raftRule.tick(memberId, Duration.ofMillis(50)),
          0.2,
          "tick 50ms",
          memberId);
      addWithProbability(
          pmf, () -> raftRule.tickElectionTimeout(memberId), 0.01, "tickElectionTimeout", memberId);
      addWithProbability(
          pmf, () -> raftRule.clientAppend(memberId), 0.1, "clientAppend", memberId);
      serverIds.forEach(
          other -> {
            if (other != memberId) {
              addWithProbability(
                  pmf,
                  () -> raftRule.getServerProtocol(memberId).deliverAll(other),
                  0.3,
                  "deliverAll to " + other.id(),
                  memberId);
              addWithProbability(
                  pmf,
                  () -> raftRule.getServerProtocol(memberId).deliverNextMessage(other),
                  0.05,
                  "deliverNext to " + other.id(),
                  memberId);
              // addWithProbability(
              //  pmf, () -> raftRule.getServerProtocol(memberId).dropNextMessage(other), 0.05,
              // "runUntildone", memberId);
            }
          });
    });*/
    addWithProbability(pmf, (m) -> raftRule.clientAppendOnLeader(), 0.1, "appendOnLeader");

    addWithProbability(pmf, (memberId) -> raftRule.runUntilDone(memberId), 0.1, "runUntildone");
    addWithProbability(pmf, (memberId) -> raftRule.runNextTask(memberId), 0.1, "runNextTask");
    addWithProbability(
        pmf, (memberId) -> raftRule.processAllMessage(memberId), 0.1, "processAllMessage");
    addWithProbability(
        pmf, (memberId) -> raftRule.processNextMessage(memberId), 0.1, "processNextMessage");
    addWithProbability(
        pmf, (memberId) -> raftRule.tickHeartbeatTimeout(memberId), 0.1, "tickHeartBeat");
    addWithProbability(
        pmf, (memberId) -> raftRule.tick(memberId, Duration.ofMillis(50)), 0.1, "tick 50ms");
    addWithProbability(
        pmf, (memberId) -> raftRule.tickElectionTimeout(memberId), 0.1, "tickElectionTimeout");
    addWithProbability(pmf, (memberId) -> raftRule.clientAppend(memberId), 0.1, "clientAppend");
    addWithProbability(
        pmf,
        (memberId) -> raftRule.getServerProtocol(memberId).deliverAll(serverIds.get(0)),
        0.1,
        "deliverAll to " + serverIds.get(0).id());
    addWithProbability(
        pmf,
        (memberId) -> raftRule.getServerProtocol(memberId).deliverAll(serverIds.get(1)),
        0.1,
        "deliverAll to " + serverIds.get(1).id());
    addWithProbability(
        pmf,
        (memberId) -> raftRule.getServerProtocol(memberId).deliverAll(serverIds.get(2)),
        0.1,
        "deliverAll to " + serverIds.get(2).id());
    addWithProbability(
        pmf,
        (memberId) -> raftRule.getServerProtocol(memberId).deliverNextMessage(serverIds.get(0)),
        0.1,
        "deliverNext to " + serverIds.get(0).id());
    addWithProbability(
        pmf,
        (memberId) -> raftRule.getServerProtocol(memberId).deliverNextMessage(serverIds.get(1)),
        0.1,
        "deliverNext to " + serverIds.get(1).id());
    addWithProbability(
        pmf,
        (memberId) -> raftRule.getServerProtocol(memberId).deliverNextMessage(serverIds.get(2)),
        0.1,
        "deliverNext to " + serverIds.get(2).id());

    addWithProbability(
        pmf,
        (memberId) -> raftRule.getServerProtocol(memberId).dropNextMessage(serverIds.get(0)),
        0.05,
        "dropNextMessage to " + serverIds.get(0));
    addWithProbability(
        pmf,
        (memberId) -> raftRule.getServerProtocol(memberId).dropNextMessage(serverIds.get(1)),
        0.05,
        "dropNextMessage to " + serverIds.get(1));
    addWithProbability(
        pmf,
        (memberId) -> raftRule.getServerProtocol(memberId).dropNextMessage(serverIds.get(2)),
        0.05,
        "dropNextMessage to " + serverIds.get(2));

    final EnumeratedDistribution<RaftOperation> distribution =
        new EnumeratedDistribution<RaftOperation>(pmf);

    final EnumeratedDistribution<MemberId> memberDistribution =
        new EnumeratedDistribution<MemberId>(
            List.of(
                new Pair<>(MemberId.from("0"), 0.3),
                new Pair<>(MemberId.from("1"), 0.3),
                new Pair<>(MemberId.from("2"), 0.3)));

    final int stepCount = 100000;
    final var randomOperations = distribution.sample(stepCount, new RaftOperation[stepCount]);
    for (final RaftOperation operation : randomOperations) {
      if (operation != null) {
        operation.run(memberDistribution.sample());
        raftRule.assertOnlyOneLeader();
      }
    }
    raftRule.assertAllLogsEqual();
  }

  private class RaftOperation {

    final Consumer<MemberId> operation;
    private final String name;

    private RaftOperation(final String name, final Consumer<MemberId> operation) {
      this.name = name;
      this.operation = operation;
    }

    public void run(final MemberId memberId) {
      LoggerFactory.getLogger("TEST").info("Running {} on {}", name, memberId);
      operation.accept(memberId);
    }
  }
}
