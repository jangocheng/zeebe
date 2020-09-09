package io.atomix.raft;

import io.atomix.raft.impl.RaftSingleThreadContextFactory;
import io.atomix.utils.concurrent.SingleThreadContext;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import org.jmock.lib.concurrent.DeterministicScheduler;

public class DeterministicSingleThreadContextFactory extends SingleThreadContext
    implements RaftSingleThreadContextFactory {
  protected DeterministicSingleThreadContextFactory(
      final ScheduledExecutorService executor,
      final Consumer<Throwable> uncaughtExceptionObserver) {
    super(executor, uncaughtExceptionObserver);
  }

  @Override
  public SingleThreadContext createContext(
      final ThreadFactory factory, final Consumer<Throwable> unCaughtExceptionHandler) {
    return new DeterministicSingleThreadContextFactory(
        new DeterministicScheduler(), unCaughtExceptionHandler);
  }
}
