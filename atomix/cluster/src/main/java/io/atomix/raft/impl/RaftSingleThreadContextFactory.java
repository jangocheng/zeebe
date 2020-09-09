package io.atomix.raft.impl;

import io.atomix.utils.concurrent.ThreadContext;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

public interface RaftSingleThreadContextFactory {
  ThreadContext createContext(ThreadFactory factory, Consumer<Throwable> unCaughtExceptionHandler);
}
