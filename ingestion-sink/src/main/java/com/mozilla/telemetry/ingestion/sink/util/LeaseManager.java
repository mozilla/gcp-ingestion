package com.mozilla.telemetry.ingestion.sink.util;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import io.opencensus.stats.Aggregation;
import io.opencensus.stats.Measure.MeasureLong;
import io.opencensus.stats.Stats;
import io.opencensus.stats.StatsRecorder;
import io.opencensus.stats.View;
import io.opencensus.stats.ViewManager;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaseManager
    extends BatchWrite<LeaseManager.Lease, LeaseManager.Lease, LeaseManager.Action, Void> {

  private static final Logger LOG = LoggerFactory.getLogger(LeaseManager.class);

  private static final int MAX_ACK_IDS_PER_REQUEST = 1000;
  private static final int MAX_ACK_DEADLINE_SECONDS = 600;

  private final SubscriberStub client;
  private final String subscription;
  private final Duration renewFrequency;
  private final int ackDeadlineSeconds;
  private final long ackDeadlineNanos;
  private final long maxApiDurationNanos;

  /** Constructor. */
  public LeaseManager(SubscriberStub client, String subscription, Duration ackDeadline,
      Duration maxDelay, Duration maxApiDuration, Executor executor) {
    super(0, MAX_ACK_IDS_PER_REQUEST, maxDelay, null, executor);
    this.client = client;
    this.subscription = subscription;
    ackDeadlineSeconds = (int) ackDeadline.getSeconds();
    Preconditions.checkArgument(ackDeadlineSeconds <= MAX_ACK_DEADLINE_SECONDS,
        String.format("ack deadline must be less than %d seconds", MAX_ACK_DEADLINE_SECONDS));
    ackDeadlineNanos = ackDeadline.toNanos();
    // renew when the batch maxDelay will end no later than halfway through ackDeadline
    renewFrequency = ackDeadline.dividedBy(2).minus(maxDelay);
    // leases with less than this much time left before expiration when a batch close starts are
    // dropped from the batch
    maxApiDurationNanos = maxApiDuration.toNanos();
  }

  private static final StatsRecorder STATS_RECORDER = Stats.getStatsRecorder();
  private static final Aggregation.Sum SUM_AGG = Aggregation.Sum.create();
  private static final MeasureLong measureAckCount = MeasureLong.create("lease_manager_ack_count",
      "The number of bytes received in ", "1");
  private static final MeasureLong measureDropped = MeasureLong.create("lease_manager_dropped",
      "The number of bytes received in ", "1");
  private static final MeasureLong measureNackCount = MeasureLong.create("lease_manager_nack_count",
      "The number of bytes received in ", "1");
  private static final MeasureLong measureRenewCount = MeasureLong
      .create("lease_manager_renew_count", "The number of bytes received in ", "1");

  /**
   * Register a view for every measure.
   *
   * <p>If this is not called, e.g. during unit tests, recorded values will not be exported.
   */
  public LeaseManager withOpenCensusMetrics() {
    final ViewManager viewManager = Stats.getViewManager();
    ImmutableMap.<MeasureLong, Aggregation>builder().put(measureAckCount, SUM_AGG)
        .put(measureDropped, SUM_AGG).put(measureNackCount, SUM_AGG).put(measureRenewCount, SUM_AGG)
        .build()
        .forEach((measure, aggregation) -> viewManager
            .registerView(View.create(View.Name.create(measure.getName()), measure.getDescription(),
                measure, aggregation, ImmutableList.of())));
    return this;
  }

  /** Create a managed lease. */
  public AckReplyConsumer lease(String ackId, long expiration, CompletableFuture<Void> renewTimer) {
    return new Lease(ackId, expiration).hold(renewTimer);
  }

  @Override
  public void flush() {
    batches.keySet().stream().sorted().map(batches::get).map(BatchWrite.Batch::flush)
        .forEach(CompletableFuture::join);
  }

  @Override
  protected Action getBatchKey(Lease lease) {
    return lease.action;
  }

  @Override
  protected Lease encodeInput(Lease input) {
    return input;
  }

  @Override
  protected Batch getBatch(Action action) {
    return new Batch(this, action);
  }

  private class Batch extends BatchWrite<Lease, Lease, Action, Void>.Batch {

    private final LeaseManager manager;
    private final Action action;
    private final List<Lease> leases = new LinkedList<>();

    private Batch(LeaseManager manager, Action action) {
      this.manager = manager;
      this.action = action;
    }

    @Override
    protected CompletableFuture<Void> close() {
      // renewFrequency+maxDelay is half of the ack deadline for a message, but under heavy load
      // this method may not be executed immediately. Prevent acting on expired leases by filtering
      // out those that expire before another maxApiDurationNanos have passed.
      final long minTimeout = System.nanoTime() + maxApiDurationNanos;
      final List<Lease> openLeases = leases.stream().filter(l -> l.expiration >= minTimeout)
          .collect(Collectors.toList());
      final int dropped = leases.size() - openLeases.size();
      STATS_RECORDER.newMeasureMap().put(action.getMeasure(), openLeases.size())
          .put(measureDropped, dropped).record();
      if (dropped > 0 && action != Action.nack) {
        String timeoutSeconds = leases.stream().filter(l -> l.expiration < minTimeout)
            .map(l -> Long.toString(Duration.ofNanos(minTimeout - l.expiration).getSeconds()))
            .collect(Collectors.joining(", "));
        LOG.warn(String.format("could not %s %d leases due to timeout; seconds since expired: %s",
            action, dropped, timeoutSeconds));
      }
      if (openLeases.size() > 0) {
        final List<String> ackIds = openLeases.stream().map(l -> l.ackId)
            .collect(Collectors.toList());
        final TimedFuture renewTimer = new TimedFuture(renewFrequency);
        // calculate expiration as late as possible
        final long expiration = System.nanoTime() + ackDeadlineNanos;
        // try {
        action.apply(manager, ackIds);
        // } catch (RuntimeException e) {
        // // TODO be more specific about the kinds of errors this retries
        // // retry immediately
        // final CompletableFuture<Void> completed = CompletableFuture.completedFuture(null);
        // openLeases.forEach(l -> l.hold(completed));
        // throw e;
        // }
        if (action == Action.renew) {
          // TODO limit the amount of time a lease may be held
          // schedule next renew
          openLeases.forEach(l -> {
            l.expiration = expiration;
            l.hold(renewTimer);
          });
        }
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    protected void write(Lease lease) {
      leases.add(lease);
    }

    @Override
    protected long getByteSize(Lease lease) {
      return 0; // TODO limit batch size based on lease.ackId.length()
    }
  }

  public class Lease implements AckReplyConsumer {

    private final String ackId;
    private long expiration;
    // new leases act the same as currently renewing leases
    private Action action = Action.renew;

    private Lease(String ackId, long expiration) {
      this.ackId = ackId;
      this.expiration = expiration;
    }

    @Override
    public synchronized void ack() {
      if (action == Action.renew) {
        // wait for renew batch to complete, then have this::hold add it to the next ack batch
        action = Action.ack;
      } else if (action == null) {
        // add to next ack batch
        action = Action.ack;
        apply(this);
      }
    }

    @Override
    public synchronized void nack() {
      if (action == Action.renew) {
        // wait for renew batch to complete, then have this::hold add it to the next nack batch
        action = Action.nack;
      } else if (action == null) {
        // lease is not part of any batch, add to next nack batch
        action = Action.nack;
        apply(this);
      }
    }

    private synchronized Lease hold(CompletableFuture<Void> renewTimer) {
      if (action == Action.renew) {
        // add to next renew batch after renewTimer completes
        action = null;
        renewTimer.thenAccept(v -> this.renew());
      } else {
        // add to next ack or nack batch now that renew is complete
        apply(this);
      }
      return this;
    }

    private synchronized void renew() {
      if (action == null) {
        // add to next renew batch because ack or nack has not yet occurred
        action = Action.renew;
        apply(this);
      }
    }
  }

  enum Action {
    renew {

      @Override
      public void apply(LeaseManager manager, List<String> ackIds) {
        manager.client.modifyAckDeadlineCallable()
            .call(ModifyAckDeadlineRequest.newBuilder().setSubscription(manager.subscription)
                .addAllAckIds(ackIds).setAckDeadlineSeconds(manager.ackDeadlineSeconds).build());
      }

      @Override
      public MeasureLong getMeasure() {
        return measureRenewCount;
      }
    },
    ack {

      @Override
      public void apply(LeaseManager manager, List<String> ackIds) {
        manager.client.acknowledgeCallable().call(AcknowledgeRequest.newBuilder()
            .setSubscription(manager.subscription).addAllAckIds(ackIds).build());
      }

      @Override
      public MeasureLong getMeasure() {
        return measureAckCount;
      }
    },
    nack {

      private static final int NACK_DEADLINE_SECONDS = 0;

      @Override
      public void apply(LeaseManager manager, List<String> ackIds) {
        manager.client.modifyAckDeadlineCallable()
            .call(ModifyAckDeadlineRequest.newBuilder().setSubscription(manager.subscription)
                .addAllAckIds(ackIds).setAckDeadlineSeconds(NACK_DEADLINE_SECONDS).build());
      }

      @Override
      public MeasureLong getMeasure() {
        return measureNackCount;
      }
    };

    public abstract void apply(LeaseManager manager, List<String> ackIds);

    public abstract MeasureLong getMeasure();
  }
}
