package io.atomix.raft.impl;

import io.atomix.utils.concurrent.SingleThreadContext;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

public interface RaftSingleThreadContextFactory {
  SingleThreadContext createContext(
      ThreadFactory factory, Consumer<Throwable> unCaughtExceptionHandler);
}
