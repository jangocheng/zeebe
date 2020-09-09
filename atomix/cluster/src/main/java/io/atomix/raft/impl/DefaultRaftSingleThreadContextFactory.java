package io.atomix.raft.impl;

import io.atomix.utils.concurrent.SingleThreadContext;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

public class DefaultRaftSingleThreadContextFactory implements RaftSingleThreadContextFactory{


  @Override
  public SingleThreadContext createContext(final ThreadFactory factory,
      final Consumer<Throwable> unCaughtExceptionHandler) {
    return new SingleThreadContext(factory, unCaughtExceptionHandler);
  }
}
