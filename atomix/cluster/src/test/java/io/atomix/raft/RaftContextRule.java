package io.atomix.raft;

import static org.mockito.Mockito.mock;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.raft.impl.RaftContext;
import io.atomix.raft.partition.impl.RaftNamespaces;
import io.atomix.raft.protocol.ControllableRaftServerProtocol;
import io.atomix.raft.protocol.RaftMessage;
import io.atomix.raft.storage.RaftStorage;
import io.atomix.storage.StorageLevel;
import io.zeebe.util.collection.Tuple;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.After;
import org.junit.Before;
import org.junit.rules.TemporaryFolder;

public class RaftContextRule {

  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private Map<MemberId, ControllableRaftServerProtocol> serverProtocols;
  private Map<MemberId, Queue<Tuple<RaftMessage, Runnable>>> messageQueue;
  private Map<MemberId, DeterministicSingleThreadContext> deterministicExecutors;
  private Path directory;

  private final int nodeCount;
  private final Map<MemberId, RaftContext> raftServers = new HashMap<>();


  public RaftContextRule(final int nodeCount) {
    this.nodeCount = nodeCount;
  }

  @Before
  public void before() throws IOException {
    directory = temporaryFolder.newFolder().toPath();
    if (nodeCount > 0) {
      createRaftContexts(nodeCount);
    }
  }

  @After
  public void after() {
    raftServers.forEach((m, c) -> c.close());
    raftServers.clear();
    serverProtocols.clear();
    deterministicExecutors.forEach((m, e) -> e.close());
    deterministicExecutors.clear();
  }

  private void createRaftContexts(final int nodeCount) {

    for (int i = 0; i < nodeCount; i++) {
      final var memberId = MemberId.from(String.valueOf(i));
      raftServers.put(memberId, createRaftContext(memberId));
    }
  }

  public RaftContext createRaftContext(final MemberId memberId) {
    return new RaftContext(
        "partition-1",
        memberId,
        mock(ClusterMembershipService.class),
        new ControllableRaftServerProtocol(memberId, serverProtocols, messageQueue),
        createStorage(MemberId.from("0")),
        DeterministicSingleThreadContext::createContext);
  }

  private RaftStorage createStorage(final MemberId memberId) {
    return createStorage(memberId, Function.identity());
  }

  private RaftStorage createStorage(
      final MemberId memberId,
      final Function<RaftStorage.Builder, RaftStorage.Builder> configurator) {
    final var memberDirectory = getMemberDirectory(directory, memberId.toString());
    final RaftStorage.Builder defaults =
        RaftStorage.builder()
            .withStorageLevel(StorageLevel.DISK)
            .withDirectory(memberDirectory)
            .withMaxEntriesPerSegment(10)
            .withMaxSegmentSize(1024 * 10)
            .withFreeDiskSpace(100)
            .withNamespace(RaftNamespaces.RAFT_STORAGE);
    return configurator.apply(defaults).build();
  }

  private File getMemberDirectory(final Path directory, final String s) {
    return new File(directory.toFile(), s);
  }

  public ControllableRaftServerProtocol getServerProtocol(final MemberId memberId) {
    return serverProtocols.get(memberId);
  }

  public DeterministicScheduler getDeterministicScheduler(final MemberId memberId) {
    return deterministicExecutors.get(memberId).getDeterministicScheduler();
  }
}
