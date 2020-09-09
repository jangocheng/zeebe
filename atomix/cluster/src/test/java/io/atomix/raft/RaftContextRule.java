package io.atomix.raft;

import static org.mockito.Mockito.mock;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.raft.impl.RaftContext;
import io.atomix.raft.partition.impl.RaftNamespaces;
import io.atomix.raft.storage.RaftStorage;
import io.atomix.storage.StorageLevel;
import java.io.File;
import java.nio.file.Path;
import java.util.function.Function;

public class RaftContextRule {

  public void createRaftContext() {
    final RaftContext raftContext = new RaftContext("partition-1", MemberId.from("0"), mock(
        ClusterMembershipService.class), createStorage(MemberId.from("0")), )

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


}
