package com.mozilla.telemetry.ingestion.sink.io;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.DeadlineExceededException;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.v1.GetSubscriptionRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.ReceivedMessage;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToTemplatedString;
import com.mozilla.telemetry.ingestion.sink.util.BatchWrite;
import com.mozilla.telemetry.ingestion.sink.util.CompletableFutures;
import com.mozilla.telemetry.ingestion.sink.util.LeaseManager;
import com.mozilla.telemetry.ingestion.sink.util.TimedFuture;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Pubsub {

  private static <T> MessageReceiver getReceiver(Logger logger,
      Function<PubsubMessage, PubsubMessage> decompress,
      Function<PubsubMessage, CompletableFuture<T>> output) {
    // Synchronous CompletableFuture methods are executed by the thread that completes the
    // future, or the current thread if the future is already complete. Use that here to
    // minimize memory usage by doing as much work as immediately possible.
    return (message, consumer) -> CompletableFuture.completedFuture(message).thenApply(decompress)
        .thenCompose(output).whenComplete((result, exception) -> {
          if (exception == null) {
            consumer.ack();
          } else {
            // exception is always a CompletionException caused by the real exception
            logger.warn("Exception while attempting to deliver message", exception.getCause());
            consumer.nack();
          }
        });
  }

  public static class Pull implements Input {

    private static final Logger LOG = LoggerFactory.getLogger(Pull.class);

    @VisibleForTesting
    final LeaseManager leaseManager;

    private final SubscriberStub client;
    private final String subscription;
    private final Duration initialRenewDelay;
    private final long initialDeadlineNanos;
    private final MessageReceiver callback;
    private final Executor executor;
    private final int parallelism;
    private final BatchWrite output;
    private boolean running;
    private final long maxOutstandingElementCount;
    private final long maxOutstandingRequestBytes;
    private final AtomicLong outstandingElementCount = new AtomicLong(0);
    private final AtomicLong outstandingRequestBytes = new AtomicLong(0);

    /** Constructor. */
    public <T> Pull(SubscriberStub client, String subscription,
        Function<PubsubMessage, CompletableFuture<T>> output,
        Function<PubsubMessage, PubsubMessage> decompress, Executor executor, int parallelism,
        long maxOutstandingElementCount, long maxOutstandingRequestBytes,
        LeaseManager leaseManager) {
      this.client = client;
      this.subscription = subscription;
      Duration initialDeadline = Duration.ofSeconds(client.getSubscriptionCallable()
          .call(GetSubscriptionRequest.newBuilder().setSubscription(subscription).build())
          .getAckDeadlineSeconds());
      initialRenewDelay = initialDeadline.dividedBy(2);
      initialDeadlineNanos = initialDeadline.toNanos();
      callback = getReceiver(LOG, decompress, output);
      this.executor = executor;
      this.parallelism = parallelism;
      this.leaseManager = leaseManager;
      this.maxOutstandingElementCount = maxOutstandingElementCount;
      this.maxOutstandingRequestBytes = maxOutstandingRequestBytes;
      if (output instanceof BatchWrite) {
        this.output = (BatchWrite) output;
      } else {
        this.output = null;
      }
    }

    private static final Random rand = new Random();
    private final Set<TimedFuture> backoffTimers = new HashSet<>();

    /** Stop pulling new messages, and immediately complete any backoffTimers. */
    public void stop() {
      // stop any running subscribers
      running = false;
      final List<TimedFuture> earlyBackoffTimers;
      synchronized (this.backoffTimers) {
        earlyBackoffTimers = new ArrayList<>(this.backoffTimers);
      }
      earlyBackoffTimers.forEach(t -> t.complete(null));
    }

    private void backoff() {
      CompletableFuture<Void> future;
      synchronized (backoffTimers) {
        if (running) {
          // wait for 5-15 seconds
          final TimedFuture backoffTimer = new TimedFuture(
              Duration.ofSeconds(5 + rand.nextInt(10)));
          backoffTimers.add(backoffTimer);
          future = backoffTimer.thenAccept(v -> {
            synchronized (backoffTimers) {
              backoffTimers.remove(backoffTimer);
            }
          });
        } else {
          future = CompletableFuture.completedFuture(null);
        }
      }
      future.join();
    }

    private List<ReceivedMessage> pull() {
      try {
        return client
            .pullCallable().call(PullRequest.newBuilder().setMaxMessages(10_000)
                .setReturnImmediately(true).setSubscription(subscription).build())
            .getReceivedMessagesList();
      } catch (DeadlineExceededException ignore) {
        return null;
      }
    }

    private void allocateSubscriberCapacity(long byteSize) {
      outstandingElementCount.incrementAndGet();
      outstandingRequestBytes.addAndGet(byteSize);
    }

    private void freeSubscriberCapacity(long byteSize) {
      outstandingElementCount.decrementAndGet();
      outstandingRequestBytes.addAndGet(-byteSize);
    }

    private class LeaseWithSubscriberCapacity implements AckReplyConsumer {

      final long byteSize;
      final AckReplyConsumer lease;

      private LeaseWithSubscriberCapacity(long byteSize, AckReplyConsumer lease) {
        allocateSubscriberCapacity(byteSize);
        this.byteSize = byteSize;
        this.lease = lease;
      }

      @Override
      public void ack() {
        freeSubscriberCapacity(byteSize);
        lease.ack();
      }

      @Override
      public void nack() {
        freeSubscriberCapacity(byteSize);
        lease.nack();
      }
    }

    private void subscriber() {
      try {
        while (running) {
          if (outstandingElementCount.get() >= maxOutstandingElementCount
              || outstandingRequestBytes.get() >= maxOutstandingRequestBytes) {
            // prevent reading more messages without blocking already pulled messages
            backoff();
          } else {
            TimedFuture renewTimer = new TimedFuture(initialRenewDelay);
            long expiration = System.nanoTime() + initialDeadlineNanos;
            List<ReceivedMessage> response = pull();
            if (response != null && response.size() > 0) {
              // add response to leaseManager and allocate subscriber capacity before executing
              // callback, so that lease renew is not blocked by synchronous parts of callback
              List<AckReplyConsumer> leases = response.stream()
                  .map(r -> new LeaseWithSubscriberCapacity(r.getMessage().getSerializedSize(),
                      leaseManager.lease(r.getAckId(), expiration, renewTimer)))
                  .collect(Collectors.toList());
              for (int i = 0; i < response.size(); i++) {
                callback.receiveMessage(response.get(i).getMessage(), leases.get(i));
              }
            } else {
              backoff();
            }
          }
        }
      } finally {
        stop();
      }
    }

    /** Run the subscriber until terminated. */
    public void run() {
      running = true;
      List<CompletableFuture<Void>> subscribers = Stream
          .generate(() -> CompletableFuture.runAsync(this::subscriber, executor)).limit(parallelism)
          .collect(Collectors.toList());
      try {
        CompletableFutures.joinAllThenThrow(subscribers);
      } finally {
        // flush outputs then flush lease manager
        if (output != null) {
          output.flush();
        }
        leaseManager.flush();
      }
    }
  }

  public static class StreamingPull implements Input {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingPull.class);

    private Subscriber subscriber;

    /** Constructor. */
    public <T> StreamingPull(String subscriptionName,
        Function<PubsubMessage, CompletableFuture<T>> output,
        Function<Subscriber.Builder, Subscriber.Builder> config,
        Function<PubsubMessage, PubsubMessage> decompress) {
      ProjectSubscriptionName subscription = ProjectSubscriptionName.parse(subscriptionName);
      subscriber = config
          .apply(Subscriber.newBuilder(subscription, getReceiver(LOG, decompress, output))).build();
    }

    public void stop() {
      subscriber.stopAsync();
    }

    /** Run the subscriber until terminated. */
    public void run() {
      try {
        subscriber.startAsync();
        subscriber.awaitTerminated();
      } finally {
        subscriber.stopAsync();
      }
    }
  }

  public static class Write implements Function<PubsubMessage, CompletableFuture<String>> {

    private final Executor executor;
    private final Function<Publisher.Builder, Publisher.Builder> config;
    private final Function<PubsubMessage, PubsubMessage> compress;
    private final PubsubMessageToTemplatedString topicTemplate;
    private final ConcurrentMap<String, Publisher> publishers = new ConcurrentHashMap<>();

    /** Constructor. */
    public Write(String topicTemplate, Executor executor,
        Function<Publisher.Builder, Publisher.Builder> config,
        Function<PubsubMessage, PubsubMessage> compress) {
      this.executor = executor;
      this.topicTemplate = PubsubMessageToTemplatedString.of(topicTemplate);
      this.config = config;
      this.compress = compress;
    }

    private Publisher getPublisher(PubsubMessage message) {
      return publishers.compute(topicTemplate.apply(message), (topic, publisher) -> {
        if (publisher == null) {
          try {
            return config.apply(Publisher.newBuilder(ProjectTopicName.parse(topic))).build();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
        return publisher;
      });
    }

    @Override
    public CompletableFuture<String> apply(PubsubMessage message) {
      final PubsubMessage compressed = compress.apply(message);
      final ApiFuture<String> future = getPublisher(message).publish(compressed);
      final CompletableFuture<String> result = new CompletableFuture<>();
      ApiFutures.addCallback(future, new ApiFutureCallback<String>() {

        @Override
        public void onFailure(Throwable throwable) {
          result.completeExceptionally(throwable);
        }

        @Override
        public void onSuccess(String messageId) {
          result.complete(messageId);
        }
      }, executor);
      return result;
    }

    public CompletableFuture<Void> withoutResult(PubsubMessage message) {
      return apply(message).thenAccept(result -> {
      });
    }
  }
}
