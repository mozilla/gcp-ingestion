/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.google.common.primitives.Ints;
import com.mozilla.telemetry.transforms.MapElementsWithErrors;
import com.mozilla.telemetry.transforms.ResultWithErrors;
import com.mozilla.telemetry.util.Time;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.UUID;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import redis.clients.jedis.Jedis;

/**
 * Base class for deduplicating messages.
 *
 * <p>Subclasses are provided via static members removeDuplicates() and markAsSeen().
 *
 * <p>The packaging of subclasses here follows the style guidelines as captured in
 * https://beam.apache.org/contribute/ptransform-style-guide/#packaging-a-family-of-transforms
 */
public abstract class Deduplicate extends MapElementsWithErrors.ToPubsubMessageFrom<PubsubMessage> {

  private final ValueProvider<URI> uri;
  private transient Jedis jedis;

  Deduplicate(ValueProvider<URI> uri) {
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

  /**
   * Get {@code document_id} attribute from {@link PubsubMessage} as {@code byte[]}.
   *
   * @exception IllegalArgumentException if {@code document_id} is an invalid {@link UUID}.
   */
  static Optional<byte[]> getId(PubsubMessage element) {
    return Optional.ofNullable(element.getAttributeMap())
        .flatMap(m -> Optional.ofNullable(m.get("document_id"))).map(UUID::fromString)
        .map(id -> ByteBuffer.wrap(new byte[16]).putLong(id.getLeastSignificantBits())
            .putLong(id.getMostSignificantBits()).array());
  }

  /*
   * Static factory methods.
   */

  /**
   * Returns a {@link PTransform} that checks message IDs with Redis and discards any messages
   * with IDs that have already been seen. The error collection contains messages with poorly-formed
   * IDs or where Redis was not available.
   */
  public static //
      PTransform<PCollection<PubsubMessage>, ResultWithErrors<PCollection<PubsubMessage>>> //
      removeDuplicates(ValueProvider<URI> uri) {
    return PTransform.compose((PCollection<PubsubMessage> input) -> {
      ResultWithErrors<PCollection<PubsubMessage>> result = input.apply(new RemoveDuplicates(uri));
      PCollection<PubsubMessage> errorsStrippedOfDuplicates = result.errors()
          .apply(Filter.by((PubsubMessage message) -> !Optional
              .ofNullable(message.getAttribute("exception_class"))
              .filter(RemoveDuplicates.DuplicateIdException.class.getName()::equals).isPresent()));
      return ResultWithErrors.of(result.output(), errorsStrippedOfDuplicates);
    });
  }

  public static Deduplicate markAsSeen(ValueProvider<URI> uri, ValueProvider<Integer> ttlSeconds) {
    return new MarkAsSeen(uri, ttlSeconds);
  }

  /*
   * Concrete subclasses.
   */

  /**
   * {@link PTransform} that redirects messages already seen to {@code errorTag}.
   */
  private static class RemoveDuplicates extends Deduplicate {

    private RemoveDuplicates(ValueProvider<URI> uri) {
      super(uri);
    }

    private static class DuplicateIdException extends Exception {
    }

    @Override
    protected PubsubMessage processElement(PubsubMessage element) throws DuplicateIdException {
      // Throws IllegalArgumentException if id is present and invalid
      if (getId(element)
          // Throws JedisConnectionException if redis can't be reached
          .filter(getJedis()::exists).isPresent()) {
        // Throw DuplicateIdException if id was in redis
        throw new DuplicateIdException();
      }
      return element;
    }
  }

  /**
   * {@link PTransform} to mark messages as seen, so any duplicates are removed.
   */
  public static class MarkAsSeen extends Deduplicate {

    // This needs to match the documented @Default annotation on getDeduplicateExpireDuration.
    private static final int DEFAULT_TTL = Ints.checkedCast(Time.parseSeconds("24h"));

    final ValueProvider<Integer> ttlSeconds;

    private MarkAsSeen(ValueProvider<URI> uri, ValueProvider<Integer> ttlSeconds) {
      super(uri);
      this.ttlSeconds = ttlSeconds;
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
      return getJedis().setex(id, getTtlSeconds(), new byte[0]);
    }

    @Override
    protected PubsubMessage processElement(PubsubMessage element) {
      // Throws IllegalArgumentException if id is present and invalid
      getId(element)
          // Throws JedisConnectionException if redis can't be reached
          .map(this::setex);
      return element;
    }
  }
}
