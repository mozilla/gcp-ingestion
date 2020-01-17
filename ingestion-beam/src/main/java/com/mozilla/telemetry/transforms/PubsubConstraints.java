package com.mozilla.telemetry.transforms;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/** Class codifying the published constraints on PubsubMessage content. */
public class PubsubConstraints {

  // https://cloud.google.com/pubsub/quotas
  public static final int MAX_MESSAGE_BYTES = 10 * 1024 * 1024;
  public static final int MAX_ATTRIBUTES_PER_MESSAGE = 100;
  public static final int MAX_ATTRIBUTE_KEY_BYTES = 256;
  public static final int MAX_ATTRIBUTE_VALUE_BYTES = 1024;
  // base64 encoding leads to 4 bytes for every 3 of input, so we might get an error for
  // messages any larger than 7 MB. To allow room for attributes, we set the limit at 6 MB.
  public static final int MAX_ENCODABLE_MESSAGE_BYTES = 6 * 1024 * 1024;

  public static final String ELLIPSIS = "...";

  public static String truncateAttributeKey(String key) {
    return truncateToFitUtf8ByteLength(key, MAX_ATTRIBUTE_KEY_BYTES);
  }

  public static String truncateAttributeValue(String value) {
    return truncateToFitUtf8ByteLength(value, MAX_ATTRIBUTE_VALUE_BYTES);
  }

  /** Fills out empty payload and attributes if the message itself or components are null. */
  public static PubsubMessage ensureNonNull(PubsubMessage message) {
    if (message == null) {
      return new PubsubMessage(new byte[] {}, new HashMap<>());
    } else if (message.getPayload() == null) {
      return ensureNonNull(new PubsubMessage(new byte[] {}, message.getAttributeMap()));
    } else if (message.getAttributeMap() == null) {
      return ensureNonNull(new PubsubMessage(message.getPayload(), new HashMap<>()));
    } else {
      return message;
    }
  }

  /**
   * Return a {@link PTransform} that truncates attribute keys and values as needed to satisfy size
   * constraints.
   */
  public static TruncateAttributes truncateAttributes() {
    return new TruncateAttributes();
  }

  //////////

  private static class TruncateAttributes
      extends PTransform<PCollection<? extends PubsubMessage>, PCollection<PubsubMessage>> {

    @Override
    public PCollection<PubsubMessage> expand(PCollection<? extends PubsubMessage> input) {
      return input.apply(ParDo.of(new Fn()));
    }

    private static class Fn extends DoFn<PubsubMessage, PubsubMessage> {

      private final Counter countTruncatedKey = Metrics.counter(Fn.class, "truncated_key");
      private final Counter countTruncatedValue = Metrics.counter(Fn.class, "truncated_value");

      @ProcessElement
      public void processElement(@Element PubsubMessage message,
          OutputReceiver<PubsubMessage> out) {
        Map<String, String> attributes = new HashMap<>(message.getAttributeMap().size());
        for (Map.Entry<String, String> entry : message.getAttributeMap().entrySet()) {
          String key = truncateAttributeKey(entry.getKey());
          if (!key.equals(entry.getKey())) {
            countTruncatedKey.inc();
          }
          String value = truncateAttributeValue(entry.getValue());
          if (value != null && !value.equals(entry.getValue())) {
            countTruncatedValue.inc();
          }
          attributes.put(key, value);
        }
        out.output(new PubsubMessage(message.getPayload(), attributes));
      }
    }
  }

  /**
   * Truncates a string to the number of characters that fit in X bytes avoiding multi byte
   * characters being cut in half at the cut off point. Also handles surrogate pairs where 2
   * characters in the string is actually one literal character.
   *
   * <p>Based on:
   * https://stackoverflow.com/a/35148974/1260237
   * http://www.jroller.com/holy/entry/truncating_utf_string_to_the
   */
  private static String truncateToFitUtf8ByteLength(final String s, final int maxBytes) {
    if (s == null) {
      return null;
    }
    Charset charset = StandardCharsets.UTF_8;
    CharsetDecoder decoder = charset.newDecoder();
    byte[] sba = s.getBytes(charset);
    if (sba.length <= maxBytes) {
      return s;
    }
    final int maxTruncatedBytes = maxBytes - ELLIPSIS.getBytes(charset).length;
    // Ensure truncation by having byte buffer = maxTruncatedBytes
    ByteBuffer bb = ByteBuffer.wrap(sba, 0, maxTruncatedBytes);
    CharBuffer cb = CharBuffer.allocate(maxTruncatedBytes);
    // Ignore an incomplete character
    decoder.onMalformedInput(CodingErrorAction.IGNORE);
    decoder.decode(bb, cb, true);
    decoder.flush(cb);
    return new String(cb.array(), 0, cb.position()) + ELLIPSIS;
  }

}
