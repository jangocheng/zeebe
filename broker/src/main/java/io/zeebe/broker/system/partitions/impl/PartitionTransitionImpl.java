/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions.impl;

import com.google.common.collect.ImmutableList;
import io.zeebe.broker.Loggers;
import io.zeebe.broker.system.partitions.PartitionContext;
import io.zeebe.broker.system.partitions.PartitionStep;
import io.zeebe.broker.system.partitions.PartitionTransition;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;

public class PartitionTransitionImpl implements PartitionTransition {

  private static final Logger LOG = Loggers.SYSTEM_LOGGER;
  private static final List<PartitionStep> EMPTY_LIST = ImmutableList.of();

  private final PartitionContext context;
  private final List<PartitionStep> leaderSteps;
  private final List<PartitionStep> followerSteps;
  private final List<PartitionStep> openedSteps = new ArrayList<>();
  private CompletableActorFuture<Void> currentTransition = CompletableActorFuture.completed(null);

  public PartitionTransitionImpl(
      final PartitionContext context,
      final List<PartitionStep> leaderSteps,
      final List<PartitionStep> followerSteps) {
    this.context = context;
    this.leaderSteps = leaderSteps;
    this.followerSteps = followerSteps;
  }

  @Override
  public ActorFuture<Void> toFollower() {
    final CompletableActorFuture<Void> future = new CompletableActorFuture<>();
    currentTransition.onComplete((nothing, err) -> transition(future, followerSteps));
    return future;
  }

  @Override
  public ActorFuture<Void> toLeader() {
    final CompletableActorFuture<Void> future = new CompletableActorFuture<>();
    currentTransition.onComplete((nothing, err) -> transition(future, leaderSteps));
    return future;
  }

  @Override
  public ActorFuture<Void> toInactive() {
    final CompletableActorFuture<Void> future = new CompletableActorFuture<>();
    currentTransition.onComplete((nothing, err) -> transition(future, EMPTY_LIST));
    return future;
  }

  private void transition(
      final CompletableActorFuture<Void> future, final List<PartitionStep> steps) {
    currentTransition = future;
    closePartition()
        .onComplete(
            (nothing, err) -> {
              if (err == null) {
                installPartition(future, new ArrayList<>(steps));
              } else {
                future.completeExceptionally(err);
              }
            });
  }

  private void installPartition(
      final CompletableActorFuture<Void> future, final List<PartitionStep> steps) {
    if (steps.isEmpty()) {
      future.complete(null);
      return;
    }

    final PartitionStep step = steps.remove(0);
    step.open(context)
        .onComplete(
            (value, err) -> {
              if (err != null) {
                LOG.error("Expected to open step '{}' but failed with", step.getName(), err);
                future.completeExceptionally(err);
              } else {
                openedSteps.add(step);
                installPartition(future, steps);
              }
            });
  }

  private CompletableActorFuture<Void> closePartition() {
    // caution: this method may be called concurrently on role transition due to closing the actor
    // - first, it is called by one of the transitionTo...() methods
    // - then it is called by onActorClosing()
    final var closingSteps = new ArrayList<>(openedSteps);
    Collections.reverse(closingSteps);

    final var closingPartitionFuture = new CompletableActorFuture<Void>();
    stepByStepClosing(closingPartitionFuture, closingSteps);

    return closingPartitionFuture;
  }

  private void stepByStepClosing(
      final CompletableActorFuture<Void> future, final List<PartitionStep> steps) {
    if (steps.isEmpty()) {
      future.complete(null);
      return;
    }

    final PartitionStep step = steps.remove(0);
    LOG.debug("Closing Zeebe-Partition-{}: {}", context.getPartitionId(), step.getName());

    final ActorFuture<Void> closeFuture = step.close(context);
    closeFuture.onComplete(
        (v, t) -> {
          if (t == null) {
            LOG.debug(
                "Closing Zeebe-Partition-{}: {} closed successfully",
                context.getPartitionId(),
                step.getName());

            // remove the completed step from the list in case that the closing is interrupted
            openedSteps.remove(step);

            // closing the remaining steps
            stepByStepClosing(future, steps);
          } else {
            LOG.error(
                "Closing Zeebe-Partition-{}: {} failed to close",
                context.getPartitionId(),
                step.getName(),
                t);
            future.completeExceptionally(t);
          }
        });
  }
}
