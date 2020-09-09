package io.atomix.raft;

import static org.mockito.Mockito.mock;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.raft.impl.RaftContext;
import io.atomix.raft.partition.impl.RaftNamespaces;
import io.atomix.raft.protocol.ControllableRaftServerProtocol;
import io.atomix.raft.protocol.RaftMessage;
import io.atomix.raft.snapshot.TestSnapshotStore;
import io.atomix.raft.storage.RaftStorage;
import io.atomix.storage.StorageLevel;
import io.zeebe.util.collection.Tuple;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.After;
import org.junit.Before;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class RaftContextRule extends ExternalResource {

  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final Map<MemberId, ControllableRaftServerProtocol> serverProtocols = new HashMap<>();
  private final Map<MemberId, Queue<Tuple<RaftMessage, Runnable>>> messageQueue = new HashMap<>();
  private final Map<MemberId, DeterministicSingleThreadContext> deterministicExecutors =
      new HashMap<>();
  private Path directory;

  private final int nodeCount;
  private final Map<MemberId, RaftContext> raftServers = new HashMap<>();
  private Duration electionTimeout;

  public RaftContextRule(final int nodeCount) {
    this.nodeCount = nodeCount;
  }

  public Map<MemberId, RaftContext> getRaftServers() {
    return raftServers;
  }

  public RaftContext getRaftServer(final int memberId) {
    return raftServers.get(MemberId.from(String.valueOf(memberId)));
  }

  public RaftContext getRaftServer(final MemberId memberId) {
    return raftServers.get(memberId);
  }

  @Override
  public Statement apply(final Statement base, final Description description) {
    final var statement = super.apply(base, description);
    return temporaryFolder.apply(statement, description);
  }

  @Before
  public void before()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    directory = temporaryFolder.newFolder().toPath();
    if (nodeCount > 0) {
      createRaftContexts(nodeCount);
    }
    joinRaftServers();
    electionTimeout = getRaftServer(0).getElectionTimeout();
  }

  @After
  public void after() {
    raftServers.forEach((m, c) -> c.close());
    raftServers.clear();
    serverProtocols.clear();
    deterministicExecutors.forEach((m, e) -> e.close());
    deterministicExecutors.clear();
  }

  private void joinRaftServers() throws InterruptedException, ExecutionException, TimeoutException {
    final Set<CompletableFuture<Void>> futures = new HashSet<>();
    final var servers = getRaftServers();
    final var serverIds = new ArrayList<>(servers.keySet());
    final long electionTimeout =
        servers.get(MemberId.from(String.valueOf(0))).getElectionTimeout().toMillis();
    Collections.sort(serverIds);
    servers.forEach(
        (memberId, raftContext) -> {
          futures.add(raftContext.getCluster().bootstrap(serverIds));
        });

    getDeterministicScheduler(MemberId.from(String.valueOf(0))).runUntilIdle();
    // trigger election on 0
    getDeterministicScheduler(MemberId.from(String.valueOf(0)))
        .tick(2 * electionTimeout, TimeUnit.MILLISECONDS);
    final var joinFuture = CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    while (!joinFuture.isDone()) {
      serverIds.forEach(memberId -> getDeterministicScheduler(memberId).runUntilIdle());
    }
    joinFuture.get(1, TimeUnit.SECONDS);
  }

  private void createRaftContexts(final int nodeCount) {
    for (int i = 0; i < nodeCount; i++) {
      final var memberId = MemberId.from(String.valueOf(i));
      raftServers.put(memberId, createRaftContext(memberId));
    }
  }

  public RaftContext createRaftContext(final MemberId memberId) {
    return new RaftContext(
        memberId.id() + "-partition-1",
        memberId,
        mock(ClusterMembershipService.class),
        new ControllableRaftServerProtocol(memberId, serverProtocols, messageQueue),
        createStorage(memberId),
        (f, u) ->
            deterministicExecutors.computeIfAbsent(
                memberId,
                m ->
                    (DeterministicSingleThreadContext)
                        DeterministicSingleThreadContext.createContext(f, u)));
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
            .withSnapshotStore(new TestSnapshotStore(new AtomicReference<>()))
            .withNamespace(RaftNamespaces.RAFT_STORAGE);
    return configurator.apply(defaults).build();
  }

  private File getMemberDirectory(final Path directory, final String s) {
    return new File(directory.toFile(), s);
  }

  public ControllableRaftServerProtocol getServerProtocol(final MemberId memberId) {
    return serverProtocols.get(memberId);
  }

  public ControllableRaftServerProtocol getServerProtocol(final int memberId) {
    return getServerProtocol(MemberId.from(String.valueOf(memberId)));
  }

  public DeterministicScheduler getDeterministicScheduler(final MemberId memberId) {
    return deterministicExecutors.get(memberId).getDeterministicScheduler();
  }

  public DeterministicScheduler getDeterministicScheduler(final int memberId) {
    return getDeterministicScheduler(MemberId.from(String.valueOf(memberId)));
  }

  public void tickElectionTimeout(final int memberId) {
    getDeterministicScheduler(memberId).tick(electionTimeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  public void runUntilDone() {
    final var serverIds = raftServers.keySet();
    serverIds.forEach(memberId -> getDeterministicScheduler(memberId).runUntilIdle());
  }

  public void runUntilDone(final int memberId) {
    getDeterministicScheduler(memberId).runUntilIdle();
  }
}
