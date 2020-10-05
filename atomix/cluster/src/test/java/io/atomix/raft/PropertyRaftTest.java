package io.atomix.raft;

import io.atomix.cluster.MemberId;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.lifecycle.AfterTry;
import net.jqwik.api.lifecycle.BeforeContainer;
import net.jqwik.api.lifecycle.BeforeTry;
import org.junit.jupiter.api.io.TempDir;

public class PropertyRaftTest {

  static Collection<RaftOperation> operations;
  static Collection<MemberId> raftMembers;
  public RaftContextRule raftRule;
  @TempDir File raftDataDirectory;

  @BeforeContainer
  public static void initOperations() {
    // Need members ids to generate pair operations
    final var servers =
        IntStream.range(0, 3)
            .mapToObj(String::valueOf)
            .map(MemberId::from)
            .collect(Collectors.toList());
    operations = RandomOpGenerator.getDefaultRaftOperations(servers);
    raftMembers = Set.copyOf(servers);
  }

  @BeforeTry
  public void setUpRaftNodes() throws Exception {
    raftRule = new RaftContextRule(3);
    raftRule.before(raftDataDirectory.toPath());
  }

  @AfterTry
  public void shutDownRaftNodes() throws IOException {
    raftRule.after();
  }

  @Property(tries = 1)
  void raftProperty(
      @ForAll("raftOperations") final List<RaftOperation> raftOperations,
      @ForAll("raftMembers") final List<MemberId> raftMembers) {
    int step = 0;
    final var memberIter = raftMembers.iterator();
    for (final RaftOperation operation : raftOperations) {
      step++;

      operation.run(raftRule, memberIter.next());
      raftRule.assertOnlyOneLeader();

      if (step % 1000 == 0) { // reading logs after every operation is too slow
        raftRule.assertAllLogsEqual();
        step = 0;
      }
    }
    raftRule.assertAllLogsEqual();
  }

  @Provide
  Arbitrary<List<RaftOperation>> raftOperations() {
    final var operation = Arbitraries.of(operations);
    return operation.collect(list -> list.size() == 10);
  }

  @Provide
  Arbitrary<List<MemberId>> raftMembers() {
    final var members = Arbitraries.of(raftMembers);
    return members.collect(list -> list.size() == 10);
  }
}
