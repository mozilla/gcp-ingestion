/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.google.auto.value.AutoValue;
import com.google.common.primitives.Ints;
import com.mozilla.telemetry.metrics.PerDocTypeCounter;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.MapElementsWithErrors;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.transforms.WithErrors;
import com.mozilla.telemetry.util.Time;
import java.io.Serializable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import redis.clients.jedis.Jedis;

/**
 * Collection of transforms that interact with a Redis instance for marking seen IDs and filtering
 * out duplicate messages that have already been seen.
 *
 * <p>The packaging of nested classes here follows the style guidelines as captured in
 * https://beam.apache.org/contribute/ptransform-style-guide/#packaging-a-family-of-transforms
 */
public class Deduplicate {

  /**
   * Returns a {@link PTransform} that checks message IDs with Redis and discards any messages
   * with IDs that have already been seen. The error collection contains messages with poorly-formed
   * IDs or where Redis was not available.
   */
  public static RemoveDuplicates removeDuplicates(ValueProvider<URI> uri) {
    return new RemoveDuplicates(uri);
  }

  /**
   * Returns a {@link PTransform} that extracts document_id from each message and marks each id as
   * seen in Redis.
   */
  public static MarkAsSeen markAsSeen(ValueProvider<URI> uri, ValueProvider<Integer> ttlSeconds) {
    return new MarkAsSeen(uri, ttlSeconds);
  }

  /**
   * Implementation of {@link #removeDuplicates(ValueProvider)}.
   */
  public static class RemoveDuplicates
      extends PTransform<PCollection<PubsubMessage>, RemoveDuplicates.Result> {

    private final RedisIdService redisIdService;

    private final TupleTag<PubsubMessage> outputTag = new TupleTag<PubsubMessage>() {
    };
    private final TupleTag<PubsubMessage> duplicateTag = new TupleTag<PubsubMessage>() {
    };
    private final TupleTag<PubsubMessage> errorTag = new TupleTag<PubsubMessage>() {
    };

    RemoveDuplicates(ValueProvider<URI> uri) {
      this.redisIdService = new RedisIdService(uri);
    }

    @AutoValue
    public abstract static class Result implements PInput, POutput {

      public abstract PCollectionTuple tuple();

      public abstract TupleTag<PubsubMessage> outputTag();

      public abstract TupleTag<PubsubMessage> duplicateTag();

      public abstract TupleTag<PubsubMessage> errorTag();

      public static Result of(PCollectionTuple tuple, TupleTag<PubsubMessage> outputTag,
          TupleTag<PubsubMessage> duplicateTag, TupleTag<PubsubMessage> errorTag) {
        return new AutoValue_Deduplicate_RemoveDuplicates_Result(tuple, outputTag, duplicateTag,
            errorTag);
      }

      /**
       * Ignore the collection of duplicates and return a {@link Result} of just the
       * non-duplicate output and the errors.
       */
      public WithErrors.Result<PCollection<PubsubMessage>> ignoreDuplicates() {
        return WithErrors.Result.of(tuple().get(outputTag()), outputTag(), tuple().get(errorTag()),
            errorTag());
      }

      /**
       * Strip the payload from duplicate messages and add them to the error collection, returning
       * a {@link Result} of the non-duplicate output and the error collection.
       */
      public WithErrors.Result<PCollection<PubsubMessage>> sendDuplicateMetadataToErrors() {
        PCollection<PubsubMessage> duplicateMetadata = tuple().get(duplicateTag())
            .apply("DropDuplicatePayloads", MapElements //
                .into(TypeDescriptor.of(PubsubMessage.class))
                .via(message -> FailureMessage.of("Duplicate",
                    new PubsubMessage("".getBytes(), message.getAttributeMap()),
                    new DuplicateIdException())));
        PCollection<PubsubMessage> errors = PCollectionList.of(tuple().get(errorTag()))
            .and(duplicateMetadata)
            .apply("FlattenDuplicateMetadataAndErrors", Flatten.pCollections());
        return WithErrors.Result.of(tuple().get(outputTag()), outputTag(), errors, errorTag());
      }

      @Override
      public Pipeline getPipeline() {
        return tuple().getPipeline();
      }

      @Override
      public Map<TupleTag<?>, PValue> expand() {
        Map<TupleTag<?>, PValue> values = new HashMap<>();
        values.put(outputTag(), tuple().get(outputTag()));
        values.put(duplicateTag(), tuple().get(duplicateTag()));
        values.put(errorTag(), tuple().get(errorTag()));
        return values;
      }

      @Override
      public void finishSpecifyingOutput(String transformName, PInput input,
          PTransform<?, ?> transform) {
      }
    }

    @Override
    public Result expand(PCollection<PubsubMessage> input) {
      PCollectionTuple tuple = input.apply(ParDo.of(new Fn()).withOutputTags(outputTag,
          TupleTagList.of(duplicateTag).and(errorTag)));
      return Result.of(tuple, outputTag, duplicateTag, errorTag);
    }

    private class Fn extends DoFn<PubsubMessage, PubsubMessage> {

      @ProcessElement
      public void processElement(@Element PubsubMessage element, MultiOutputReceiver out) {
        element = PubsubConstraints.ensureNonNull(element);
        boolean idExists = false;
        boolean exceptionWasThrown = false;
        try {
          idExists = //
              // Throws IllegalArgumentException if id is present and invalid
              getId(element)
                  // Throws JedisConnectionException if redis can't be reached
                  .filter(redisIdService.getJedis()::exists).isPresent();
        } catch (Exception e) {
          exceptionWasThrown = true;
          out.get(errorTag).output(FailureMessage.of(RemoveDuplicates.this, element, e));
        }
        if (!exceptionWasThrown) {
          if (idExists) {
            PerDocTypeCounter.inc(element.getAttributeMap(), "duplicate_submission");
            PerDocTypeCounter.inc(element.getAttributeMap(), "duplicate_submission_bytes",
                element.getPayload().length);
            out.get(duplicateTag).output(element);
          } else {
            out.get(outputTag).output(element);
          }
        }
      }
    }

  }

  /**
   * Implementation of {@link #markAsSeen(ValueProvider, ValueProvider)}.
   */
  public static class MarkAsSeen extends MapElementsWithErrors.ToPubsubMessageFrom<PubsubMessage> {

    // This needs to match the documented @Default annotation on getDeduplicateExpireDuration.
    private static final int DEFAULT_TTL = Ints.checkedCast(Time.parseSeconds("24h"));

    final ValueProvider<Integer> ttlSeconds;
    private final RedisIdService redisIdService;

    MarkAsSeen(ValueProvider<URI> uri, ValueProvider<Integer> ttlSeconds) {
      this.ttlSeconds = ttlSeconds;
      this.redisIdService = new RedisIdService(uri);
    }

    /**
     * Workaround for current lack of Beam support for Default on ValueProvider.
     * If no value is specified at runtime or compile time, then the ValueProvider always reports
     * as inaccessible, even after {@code pipeline.run()} is called.
     */
    private Integer getTtlSeconds() {
      if (ttlSeconds.isAccessible()) {
        return ttlSeconds.get();
      } else {
        return DEFAULT_TTL;
      }
    }

    private String setex(byte[] id) {
      return redisIdService.getJedis().setex(id, getTtlSeconds(), new byte[0]);
    }

    @Override
    protected PubsubMessage processElement(PubsubMessage element) {
      element = PubsubConstraints.ensureNonNull(element);
      // Throws IllegalArgumentException if id is present and invalid
      getId(element)
          // Throws JedisConnectionException if redis can't be reached
          .map(this::setex);
      return element;
    }
  }

  ////////

  private static class DuplicateIdException extends Exception {

    DuplicateIdException() {
      super("A message with this documentId has already been successfully processed.");
    }
  }

  private static class RedisIdService implements Serializable {

    private final ValueProvider<URI> uri;
    private transient Jedis jedis;

    RedisIdService(ValueProvider<URI> uri) {
      this.uri = uri;
    }

    /**
     * Lazy get transient {@link Jedis} client.
     */
    Jedis getJedis() {
      if (jedis == null) {
        jedis = new Jedis(uri.get());
      }
      return jedis;
    }
  }

  /**
   * Get {@code document_id} attribute from {@link PubsubMessage} as {@code byte[]}.
   *
   * @throws IllegalArgumentException if {@code document_id} is an invalid {@link UUID}.
   */
  private static Optional<byte[]> getId(PubsubMessage element) {
    return Optional.ofNullable(element.getAttribute("document_id")).map(UUID::fromString)
        .map(id -> ByteBuffer.wrap(new byte[16]).putLong(id.getLeastSignificantBits())
            .putLong(id.getMostSignificantBits()).array());
  }
}
