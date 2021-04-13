package com.mozilla.telemetry.transforms;

import com.google.cloud.pubsublite.beam.PubsubLiteIO;
import com.google.cloud.pubsublite.proto.AttributeValues;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.protobuf.ByteString;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Functions for converting messages between standard PubSub and PubSub Lite. */
public class PubsubLiteCompat {

  /**
   * Convert message from {@link PubsubLiteIO#read} to {@link PubsubMessage}.
   * 
   * <p>For compatiblity with standard PubSub {@link PubsubMessage} extract exactly one value per
   * attribute, even though Pubsub Lite's {@link PubSubMessage} allows multiple values.
   */
  public static PubsubMessage fromPubsubLite(SequencedMessage sequencedMessage) {
    PubSubMessage message = sequencedMessage.getMessage();
    Map<String, String> attributesWithMessageId = message.getAttributesMap().entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().getValues(0).toStringUtf8()));
    if (sequencedMessage.hasCursor()) {
      attributesWithMessageId.put(Attribute.MESSAGE_ID,
          Long.toString(sequencedMessage.getCursor().getOffset()));
    }
    return new PubsubMessage(message.getData().toByteArray(), attributesWithMessageId);
  }

  /** 
   * Return {@link #fromPubsubLite(SequencedMessage)} as a {@link MapElements} transform.
   */
  public static MapElements<SequencedMessage, PubsubMessage> fromPubsubLite() {
    return MapElements.into(TypeDescriptor.of(PubsubMessage.class))
        .via(PubsubLiteCompat::fromPubsubLite);
  }

  /**
   * Convert message from {@link PubsubMessage} for {@link PubsubLiteIO.write}.
   * 
   * <p>For compatiblity with standard PubSub {@link PubsubMessage} set exactly one value per
   * attribute, even though PubSub Lite's {@link PubSubMessage} allows multiple values.
   */
  public static PubSubMessage toPubsubLite(PubsubMessage message) {
    message = PubsubConstraints.ensureNonNull(message);
    Map<String, AttributeValues> attributes = message.getAttributeMap().entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey(), e -> AttributeValues.newBuilder()
            .addValues(ByteString.copyFromUtf8(e.getValue())).build()));
    return PubSubMessage.newBuilder().setData(ByteString.copyFrom(message.getPayload()))
        .putAllAttributes(attributes).build();
  }

  /**
   * Return {@link #toPubsubLite(PubsubMessage)} as a {@link MapElements} transform.
   */
  public static MapElements<PubsubMessage, PubSubMessage> toPubsubLite() {
    return MapElements.into(TypeDescriptor.of(PubSubMessage.class))
        .via(PubsubLiteCompat::toPubsubLite);
  }
}
