package io.atomix.raft;

import static org.assertj.core.api.Assertions.assertThat;

import io.atomix.cluster.MemberId;
import io.atomix.raft.RaftServer.Role;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

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
                }
              });
        });
  }
  @Test
  public void shouldJoin() {
    assertThat(raftRule.getRaftServer(0).getRaftRole().role() == Role.LEADER);
  }

  @Test
  public void shouldBackToBackLeaderElection() {
    assertThat(raftRule.getRaftServer(0).getRaftRole().role() == Role.LEADER);
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
    final var random = new Random(1000);
    for (int i = 0; i < 1000; i++) {
      final var nextOp = Math.abs(random.nextInt());
      try {
        operations.get(nextOp % operations.size()).run();
      } catch (final Throwable e) {
        e.printStackTrace();
        throw e;
      }
    }

    raftRule.assertAllLogsEqual();
  }
}
