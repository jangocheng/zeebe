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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class RandomizedRaftTest {

  public static final int STEPCOUNT = 100000;

  @Rule
  @Parameter(0)
  public RaftContextRule raftRule;

  @Parameter(1)
  public long pmfSeed;

  @Parameter(2)
  public long operationsSeed;

  private List<Runnable> randomOperations;

  @Parameters(name = "{0}")
  public static Collection<Object[]> generateRandomOperations() {
    final List<Object[]> schedules = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      for (int j = 1; j <= 10; j++) {
        final var raftRule = new RaftContextRule(3);
        schedules.add(new Object[] {raftRule, i * System.currentTimeMillis(), j * System.currentTimeMillis()});
      }
    }
    return schedules;
  }

  @Before
  public void before() {
    final RandomOpGenerator randomOpGenerator =
        new RandomOpGenerator(
            RandomOpGenerator.getDefaultRaftOperations(raftRule),
            raftRule.getRaftServers().keySet());
    final var pmf = randomOpGenerator.generateOpProbabilityFunction(pmfSeed);
    randomOperations = randomOpGenerator.generateRandomOperations(operationsSeed, pmf, STEPCOUNT);
    raftRule
        .getRaftServers()
        .keySet()
        .forEach(s -> raftRule.getServerProtocol(s).setDeliverImmediately(false));
  }

  @After
  public void after() {
    randomOperations.clear();
  }

  @Test
  public void verifyRaftProperties() {
    int step = 0;
    for (final Runnable operation : randomOperations) {
      step++;
      if (operation != null) {
        operation.run();
        raftRule.assertOnlyOneLeader();
      }
      if (step % 1000 == 0) { // reading logs after every operation is too slow
        raftRule.assertAllLogsEqual();
        step = 0;
      }
    }
    raftRule.assertAllLogsEqual();
  }
}
