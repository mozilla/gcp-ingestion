package com.mozilla.telemetry.ingestion.sink.util;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

public class LeaseManagerTest {

  private MockPubsub pubsub;
  private LeaseManager leaseManager;
  private Duration ackDeadline = Duration.ofSeconds(600);

  /** Prepare a mock BQ response. */
  @Before
  public void mockPubsubClient() {
    pubsub = new MockPubsub();
    leaseManager = new LeaseManager(pubsub.client, "subscription", ackDeadline, Duration.ZERO,
        Duration.ZERO, ForkJoinPool.commonPool());
  }

  @Test
  public void canAckFirst() {
    String ackId = UUID.randomUUID().toString();
    pubsub.withAcknowledge();
    CompletableFuture<Void> renewTimer = new CompletableFuture<>();
    long expiration = System.nanoTime() + ackDeadline.toNanos();
    leaseManager.lease(ackId, expiration, renewTimer).ack(); // immediately ack
    leaseManager.flush(); // wait for batch to complete
    renewTimer.complete(null);
    assertEquals(ImmutableList.of(ImmutableList.of(ackId)), pubsub.acknowledgeCallable.requests
        .stream().map(AcknowledgeRequest::getAckIdsList).collect(Collectors.toList()));
    assertEquals(ImmutableList.of(), leaseManager.batches.entrySet().stream()
        .filter(e -> !e.getValue().result.isDone()).collect(Collectors.toList()));
  }

  @Test
  public void canNackFirst() {
    String ackId = UUID.randomUUID().toString();
    pubsub.withModifyAckDeadline();
    CompletableFuture<Void> renewTimer = new CompletableFuture<>();
    long expiration = System.nanoTime() + ackDeadline.toNanos();
    leaseManager.lease(ackId, expiration, renewTimer).nack(); // immediately nack
    leaseManager.flush(); // wait for batch to complete
    renewTimer.complete(null);
    assertEquals(ImmutableList.of(ImmutableList.of(ackId)),
        pubsub.modifyAckDeadlineCallable.requests.stream()
            .map(ModifyAckDeadlineRequest::getAckIdsList).collect(Collectors.toList()));
    assertEquals(ImmutableList.of(0), pubsub.modifyAckDeadlineCallable.requests.stream()
        .map(ModifyAckDeadlineRequest::getAckDeadlineSeconds).collect(Collectors.toList()));
    assertEquals(ImmutableList.of(), leaseManager.batches.entrySet().stream()
        .filter(e -> !e.getValue().result.isDone()).collect(Collectors.toList()));
  }

  @Test
  public void canRenewTwice() {
    int ackDeadlineSeconds = 10;
    Duration ackDeadline = Duration.ofSeconds(ackDeadlineSeconds);
    leaseManager = new LeaseManager(pubsub.client, "subscription", ackDeadline, Duration.ZERO,
        Duration.ZERO, ForkJoinPool.commonPool());
    String ackId = UUID.randomUUID().toString();
    pubsub.withModifyAckDeadline();
    // use completed future to ensure first renew is immediately queued
    CompletableFuture<Void> renewTimer = CompletableFuture.completedFuture(null);
    long expiration = System.nanoTime() + ackDeadline.toNanos();
    leaseManager.lease(ackId, expiration, renewTimer);
    leaseManager.flush(); // wait for batch to complete
    // check that first renew has occurred
    assertEquals(Collections.nCopies(1, ImmutableList.of(ackId)),
        pubsub.modifyAckDeadlineCallable.requests.stream()
            .map(ModifyAckDeadlineRequest::getAckIdsList).collect(Collectors.toList()));
    assertEquals(Collections.nCopies(1, ackDeadlineSeconds),
        pubsub.modifyAckDeadlineCallable.requests.stream()
            .map(ModifyAckDeadlineRequest::getAckDeadlineSeconds).collect(Collectors.toList()));
    // prepare to renew again
    pubsub.withModifyAckDeadline();
    // wait 1.5 renew cycles, and ensure we've renewed once.
    Duration renewFrequency = Duration.ofSeconds(ackDeadlineSeconds).dividedBy(2);
    new TimedFuture(renewFrequency.dividedBy(2).multipliedBy(3)).join();
    assertEquals(Collections.nCopies(2, ImmutableList.of(ackId)),
        pubsub.modifyAckDeadlineCallable.requests.stream()
            .map(ModifyAckDeadlineRequest::getAckIdsList).collect(Collectors.toList()));
    assertEquals(Collections.nCopies(2, ackDeadlineSeconds),
        pubsub.modifyAckDeadlineCallable.requests.stream()
            .map(ModifyAckDeadlineRequest::getAckDeadlineSeconds).collect(Collectors.toList()));
    // no pending batches means all leases are on hold
    assertEquals(ImmutableList.of(), leaseManager.batches.entrySet().stream()
        .filter(e -> !e.getValue().result.isDone()).collect(Collectors.toList()));
  }
}
