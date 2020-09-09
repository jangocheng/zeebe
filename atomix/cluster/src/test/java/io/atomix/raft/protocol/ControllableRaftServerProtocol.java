/*
 * Copyright 2017-present Open Networking Foundation
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.raft.protocol;

import com.google.common.collect.Sets;
import io.atomix.cluster.MemberId;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContext;
import io.zeebe.util.collection.Tuple;
import java.net.ConnectException;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** Test server protocol. */
public class ControllableRaftServerProtocol implements RaftServerProtocol {

  private Function<JoinRequest, CompletableFuture<JoinResponse>> joinHandler;
  private Function<LeaveRequest, CompletableFuture<LeaveResponse>> leaveHandler;
  private Function<ConfigureRequest, CompletableFuture<ConfigureResponse>> configureHandler;
  private Function<ReconfigureRequest, CompletableFuture<ReconfigureResponse>> reconfigureHandler;
  private Function<InstallRequest, CompletableFuture<InstallResponse>> installHandler;
  private Function<TransferRequest, CompletableFuture<TransferResponse>> transferHandler;
  private Function<PollRequest, CompletableFuture<PollResponse>> pollHandler;
  private Function<VoteRequest, CompletableFuture<VoteResponse>> voteHandler;
  private Function<AppendRequest, CompletableFuture<AppendResponse>> appendHandler;
  private final Set<MemberId> partitions = Sets.newCopyOnWriteArraySet();
  private final Map<MemberId, ControllableRaftServerProtocol> servers;
  private final ThreadContext context;
  private final Map<MemberId, Queue<Tuple<RaftMessage, Runnable>>> messageQueue;
  private final MemberId localMemberId;
  private final boolean deliverImmediately = true;

  public ControllableRaftServerProtocol(
      final MemberId memberId,
      final Map<MemberId, ControllableRaftServerProtocol> servers,
      final ThreadContext context,
      final Map<MemberId, Queue<Tuple<RaftMessage, Runnable>>> messageQueue) {
    this.servers = servers;
    this.context = context;
    this.messageQueue = messageQueue;
    localMemberId = memberId;
    servers.put(memberId, this);
  }

  public void disconnect(final MemberId target) {
    partitions.add(target);
  }

  public void reconnect(final MemberId target) {
    partitions.remove(target);
  }

  public void deliverNextMessage(final MemberId target) {
    final var nextMessage = messageQueue.get(target).poll();
    if (nextMessage != null) {
      nextMessage.getRight().run();
    }
  }

  ControllableRaftServerProtocol server(final MemberId memberId) {
    if (partitions.contains(memberId)) {
      return null;
    }
    return servers.get(memberId);
  }

  private void addToQueue(
      final MemberId memberId, final RaftMessage request, final Runnable requestHandler) {
    messageQueue.get(memberId).add(new Tuple<>(request, requestHandler));
    if (deliverImmediately) {
      deliverNextMessage(memberId);
    }
  }

  @Override
  public CompletableFuture<JoinResponse> join(final MemberId memberId, final JoinRequest request) {
    final var responseFuture = new CompletableFuture<JoinResponse>();
    addToQueue(
        memberId,
        request,
        () ->
            getServer(memberId)
                .thenCompose(listener -> listener.join(request))
                .thenAccept(
                    response ->
                        addToQueue(
                            localMemberId, response, () -> responseFuture.complete(response))));
    return responseFuture;
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(
      final MemberId memberId, final LeaveRequest request) {
    final var responseFuture = new CompletableFuture<LeaveResponse>();
    addToQueue(
        memberId,
        request,
        () ->
            getServer(memberId)
                .thenCompose(listener -> listener.leave(request))
                .thenAccept(
                    response ->
                        addToQueue(
                            localMemberId, response, () -> responseFuture.complete(response))));
    return responseFuture;
  }

  @Override
  public CompletableFuture<ConfigureResponse> configure(
      final MemberId memberId, final ConfigureRequest request) {
    final var responseFuture = new CompletableFuture<ConfigureResponse>();
    addToQueue(
        memberId,
        request,
        () ->
            getServer(memberId)
                .thenCompose(listener -> listener.configure(request))
                .thenAccept(
                    response ->
                        addToQueue(
                            localMemberId, response, () -> responseFuture.complete(response))));
    return responseFuture;
  }

  @Override
  public CompletableFuture<ReconfigureResponse> reconfigure(
      final MemberId memberId, final ReconfigureRequest request) {
    final var responseFuture = new CompletableFuture<ReconfigureResponse>();
    addToQueue(
        memberId,
        request,
        () ->
            getServer(memberId)
                .thenCompose(listener -> listener.reconfigure(request))
                .thenAccept(
                    response ->
                        addToQueue(
                            localMemberId, response, () -> responseFuture.complete(response))));
    return responseFuture;
  }

  @Override
  public CompletableFuture<InstallResponse> install(
      final MemberId memberId, final InstallRequest request) {
    final var responseFuture = new CompletableFuture<InstallResponse>();
    addToQueue(
        memberId,
        request,
        () ->
            getServer(memberId)
                .thenCompose(listener -> listener.install(request))
                .thenAccept(
                    response ->
                        addToQueue(
                            localMemberId, response, () -> responseFuture.complete(response))));
    return responseFuture;
  }

  @Override
  public CompletableFuture<TransferResponse> transfer(
      final MemberId memberId, final TransferRequest request) {
    final var responseFuture = new CompletableFuture<TransferResponse>();
    addToQueue(
        memberId,
        request,
        () ->
            getServer(memberId)
                .thenCompose(listener -> listener.transfer(request))
                .thenAccept(
                    response ->
                        addToQueue(
                            localMemberId, response, () -> responseFuture.complete(response))));
    return responseFuture;
  }

  @Override
  public CompletableFuture<PollResponse> poll(final MemberId memberId, final PollRequest request) {
    final var responseFuture = new CompletableFuture<PollResponse>();
    addToQueue(
        memberId,
        request,
        () ->
            getServer(memberId)
                .thenCompose(listener -> listener.poll(request))
                .thenAccept(
                    response ->
                        addToQueue(
                            localMemberId, response, () -> responseFuture.complete(response))));
    return responseFuture;
  }

  @Override
  public CompletableFuture<VoteResponse> vote(final MemberId memberId, final VoteRequest request) {
    final var responseFuture = new CompletableFuture<VoteResponse>();
    addToQueue(
        memberId,
        request,
        () ->
            getServer(memberId)
                .thenCompose(listener -> listener.vote(request))
                .thenAccept(
                    response ->
                        addToQueue(
                            localMemberId, response, () -> responseFuture.complete(response))));
    return responseFuture;
  }

  @Override
  public CompletableFuture<AppendResponse> append(
      final MemberId memberId, final AppendRequest request) {
    final var responseFuture = new CompletableFuture<AppendResponse>();
    addToQueue(
        memberId,
        request,
        () ->
            getServer(memberId)
                .thenCompose(listener -> listener.append(request))
                .thenAccept(
                    response ->
                        addToQueue(
                            localMemberId, response, () -> responseFuture.complete(response))));
    return responseFuture;
  }

  @Override
  public void registerJoinHandler(
      final Function<JoinRequest, CompletableFuture<JoinResponse>> handler) {
    joinHandler = handler;
  }

  @Override
  public void unregisterJoinHandler() {
    joinHandler = null;
  }

  @Override
  public void registerLeaveHandler(
      final Function<LeaveRequest, CompletableFuture<LeaveResponse>> handler) {
    leaveHandler = handler;
  }

  @Override
  public void unregisterLeaveHandler() {
    leaveHandler = null;
  }

  @Override
  public void registerTransferHandler(
      final Function<TransferRequest, CompletableFuture<TransferResponse>> handler) {
    transferHandler = handler;
  }

  @Override
  public void unregisterTransferHandler() {
    transferHandler = null;
  }

  @Override
  public void registerConfigureHandler(
      final Function<ConfigureRequest, CompletableFuture<ConfigureResponse>> handler) {
    configureHandler = handler;
  }

  @Override
  public void unregisterConfigureHandler() {
    configureHandler = null;
  }

  @Override
  public void registerReconfigureHandler(
      final Function<ReconfigureRequest, CompletableFuture<ReconfigureResponse>> handler) {
    reconfigureHandler = handler;
  }

  @Override
  public void unregisterReconfigureHandler() {
    reconfigureHandler = null;
  }

  @Override
  public void registerInstallHandler(
      final Function<InstallRequest, CompletableFuture<InstallResponse>> handler) {
    installHandler = handler;
  }

  @Override
  public void unregisterInstallHandler() {
    installHandler = null;
  }

  @Override
  public void registerPollHandler(
      final Function<PollRequest, CompletableFuture<PollResponse>> handler) {
    pollHandler = handler;
  }

  @Override
  public void unregisterPollHandler() {
    pollHandler = null;
  }

  @Override
  public void registerVoteHandler(
      final Function<VoteRequest, CompletableFuture<VoteResponse>> handler) {
    voteHandler = handler;
  }

  @Override
  public void unregisterVoteHandler() {
    voteHandler = null;
  }

  @Override
  public void registerAppendHandler(
      final Function<AppendRequest, CompletableFuture<AppendResponse>> handler) {
    appendHandler = handler;
  }

  @Override
  public void unregisterAppendHandler() {
    appendHandler = null;
  }

  private CompletableFuture<ControllableRaftServerProtocol> getServer(final MemberId memberId) {
    final ControllableRaftServerProtocol server = server(memberId);
    if (server != null) {
      return Futures.completedFuture(server);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  CompletableFuture<AppendResponse> append(final AppendRequest request) {
    if (appendHandler != null) {
      return appendHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  CompletableFuture<VoteResponse> vote(final VoteRequest request) {
    if (voteHandler != null) {
      return voteHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  CompletableFuture<PollResponse> poll(final PollRequest request) {
    if (pollHandler != null) {
      return pollHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  CompletableFuture<TransferResponse> transfer(final TransferRequest request) {
    if (transferHandler != null) {
      return transferHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  CompletableFuture<InstallResponse> install(final InstallRequest request) {
    if (installHandler != null) {
      return installHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  CompletableFuture<ReconfigureResponse> reconfigure(final ReconfigureRequest request) {
    if (reconfigureHandler != null) {
      return reconfigureHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  CompletableFuture<ConfigureResponse> configure(final ConfigureRequest request) {
    if (configureHandler != null) {
      return configureHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  CompletableFuture<LeaveResponse> leave(final LeaveRequest request) {
    if (leaveHandler != null) {
      return leaveHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  CompletableFuture<JoinResponse> join(final JoinRequest request) {
    if (joinHandler != null) {
      return joinHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }
}
