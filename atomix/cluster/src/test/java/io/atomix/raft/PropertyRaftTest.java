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

import com.google.common.io.Files;
import io.atomix.cluster.MemberId;
import io.zeebe.util.FileUtil;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.ShrinkingMode;
import net.jqwik.api.lifecycle.AfterTry;
import net.jqwik.api.lifecycle.BeforeContainer;

public class PropertyRaftTest {

  static Collection<RaftOperation> operations;
  static Collection<MemberId> raftMembers;
  private static final int OPERATION_SIZE = 100000;
  public RaftContextRule raftRule;
  File raftDataDirectory;

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

  public void setUpRaftNodes(final Random random) throws Exception {
    // Couldnot make @TempDir annotation work
    raftDataDirectory = Files.createTempDir();
    raftRule = new RaftContextRule(3);
    raftRule.before(raftDataDirectory.toPath(), random);
  }

  @AfterTry
  public void shutDownRaftNodes() throws IOException {
    raftRule.after();
    FileUtil.deleteFolder(raftDataDirectory.toPath());
    raftDataDirectory = null;
  }

  @Property(tries = 100, shrinking = ShrinkingMode.OFF)
  void raftProperty(
      @ForAll("raftOperations") final List<RaftOperation> raftOperations,
      @ForAll("raftMembers") final List<MemberId> raftMembers,
      @ForAll("randoms") final Random random)
      throws Exception {

    setUpRaftNodes(random);

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
    return operation.list().ofSize(OPERATION_SIZE);
  }

  @Provide
  Arbitrary<List<MemberId>> raftMembers() {
    final var members = Arbitraries.of(raftMembers);
    return members.list().ofSize(OPERATION_SIZE);
  }

  @Provide
  Arbitrary<Random> randoms() {
    return Arbitraries.randoms();
  }
}
