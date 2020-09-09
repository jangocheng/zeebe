package io.atomix.raft;

import static org.assertj.core.api.Assertions.assertThat;

import io.atomix.raft.RaftServer.Role;
import org.junit.Rule;
import org.junit.Test;

public class RaftControlledTest {

  @Rule public RaftContextRule raftRule = new RaftContextRule(3);

  @Test
  public void shouldJoin() {
    assertThat(raftRule.getRaftServer(0).getRaftRole().role() == Role.LEADER);
  }

  @Test
  public void shouldBackToBackLeaderElection() {
    assertThat(raftRule.getRaftServer(0).getRaftRole().role() == Role.LEADER);
    raftRule.tickElectionTimeout(1);
    raftRule.tickElectionTimeout(1);
    raftRule.runUntilDone(); // accepts poll requests
    raftRule.runUntilDone(1);
    raftRule.runUntilDone(0); // accepts vote
    raftRule.runUntilDone(2); // 2 accepts vote
    raftRule.tickElectionTimeout(2);
    raftRule.tickElectionTimeout(2); // 2 starts new election
    raftRule.runUntilDone(0); // accepts poll
    raftRule.runUntilDone(1); // 1 receives vote response
    raftRule.runUntilDone(2); // send vote
    raftRule.runUntilDone(0); // accept vote
    raftRule.runUntilDone(2);
    raftRule.runUntilDone(0);
    raftRule.runUntilDone(1);
    raftRule.runUntilDone();
  }
}
