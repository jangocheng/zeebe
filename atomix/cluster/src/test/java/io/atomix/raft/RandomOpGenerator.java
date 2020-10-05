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

import io.atomix.cluster.MemberId;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;

public class RandomOpGenerator {

  private final Collection<RaftOperation> raftOperations;
  private final Collection<MemberId> memberIds;

  public RandomOpGenerator(
      final Collection<RaftOperation> raftOperations, final Collection<MemberId> memberIds) {
    this.raftOperations = raftOperations;
    this.memberIds = memberIds;
  }

  public static Collection<RaftOperation> getDefaultRaftOperations(
      final Collection<MemberId> serverIds) {
    final Collection<RaftOperation> defaultRaftOperation = new ArrayList<>();
    defaultRaftOperation.add(
        RaftOperation.of("runNextTask", (raftRule, memberId) -> raftRule.runNextTask(memberId)));
    defaultRaftOperation.add(
        RaftOperation.of(
            "processAllMessage", (raftRule, memberId) -> raftRule.processAllMessage(memberId)));
    defaultRaftOperation.add(
        RaftOperation.of(
            "processNextMessage", (raftRule, memberId) -> raftRule.processNextMessage(memberId)));
    defaultRaftOperation.add(
        RaftOperation.of(
            "tickElectionTimeout", (raftRule, memberId) -> raftRule.tickElectionTimeout(memberId)));
    defaultRaftOperation.add(
        RaftOperation.of(
            "tickHeartBeatTimeout",
            (raftRule, memberId) -> raftRule.tickHeartbeatTimeout(memberId)));
    defaultRaftOperation.add(
        RaftOperation.of("tick 50ms", (raftRule, m) -> raftRule.tick(m, Duration.ofMillis(50))));
    defaultRaftOperation.add(
        RaftOperation.of("clientAppend", (raftRule, memberId) -> raftRule.clientAppend(memberId)));
    defaultRaftOperation.add(
        RaftOperation.of("clientAppendOnLeader", (raftRule, m) -> raftRule.clientAppendOnLeader()));

    serverIds.forEach(
        target ->
            defaultRaftOperation.add(
                RaftOperation.of(
                    "deliverAllMessage to " + target.id(),
                    (raftRule, m) -> raftRule.getServerProtocol(m).deliverAll(target))));
    serverIds.forEach(
        target ->
            defaultRaftOperation.add(
                RaftOperation.of(
                    "deliverNextMessage to " + target.id(),
                    (raftRule, m) -> raftRule.getServerProtocol(m).deliverNextMessage(target))));
    serverIds.forEach(
        target ->
            defaultRaftOperation.add(
                RaftOperation.of(
                    "dropNextMessage to " + target.id(),
                    (raftRule, m) -> raftRule.getServerProtocol(m).dropNextMessage(target))));
    return defaultRaftOperation;
  }

  public List<Pair<RaftOperation, Double>> generateOpProbabilityFunction(final long seed) {
    final var random = new Random(seed);
    final List<Pair<RaftOperation, Double>> pmf = new ArrayList<>();
    for (final RaftOperation operation : raftOperations) {
      var probability = random.nextDouble();
      if (probability == 0.0d) {
        probability = 0.001d;
      }
      pmf.add(new Pair<>(operation, probability));
    }
    return pmf;
  }

  public List<Runnable> generateRandomOperations(
      final long seed, final List<Pair<RaftOperation, Double>> pmf, final long sampleSize) {
    final List<Pair<MemberId, Double>> memberIdPmf = new ArrayList<>();
    // Add equal probability distribution of members
    memberIds.forEach(m -> memberIdPmf.add(new Pair<>(m, 0.3)));
    final EnumeratedDistribution<MemberId> memberDistribution =
        new EnumeratedDistribution<>(memberIdPmf);

    final EnumeratedDistribution<RaftOperation> distribution = new EnumeratedDistribution<>(pmf);
    distribution.reseedRandomGenerator(seed);

    final List<Runnable> operations = new ArrayList<>();
    for (int i = 0; i < sampleSize; i++) {
      final var nextOperation = distribution.sample();
      operations.add(() -> nextOperation.run(null, memberDistribution.sample()));
    }
    return operations;
  }
}
