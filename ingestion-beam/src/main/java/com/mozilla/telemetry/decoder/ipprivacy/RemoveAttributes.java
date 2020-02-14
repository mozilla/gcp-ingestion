package com.mozilla.telemetry.decoder.ipprivacy;

import com.google.common.collect.ImmutableList;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

/**
 * Remove all attributes not needed for the IP privacy dataset.
 */
public class RemoveAttributes
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  private static final Fn fn = new Fn();
  private static final RemoveAttributes INSTANCE = new RemoveAttributes();

  private static final List<String> ATTRIBUTES_TO_KEEP = ImmutableList.of(
      Attribute.SUBMISSION_TIMESTAMP, Attribute.CLIENT_ID, Attribute.CLIENT_IP,
      Attribute.GEO_COUNTRY);

  public static RemoveAttributes of() {
    return INSTANCE;
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(MapElements.via(fn));
  }

  private static class Fn extends SimpleFunction<PubsubMessage, PubsubMessage> {

    @Override
    public PubsubMessage apply(PubsubMessage message) {
      message = PubsubConstraints.ensureNonNull(message);

      final Map<String, String> attributes = message.getAttributeMap();

      Map<String, String> strippedAttributes = new HashMap<>();

      // Use geo_country to match IP privacy v1 dataset
      attributes.put(Attribute.GEO_COUNTRY,
          message.getAttribute(Attribute.NORMALIZED_COUNTRY_CODE));

      ATTRIBUTES_TO_KEEP.forEach(name -> Optional.ofNullable(attributes.get(name))
          .ifPresent(value -> strippedAttributes.put(name, value)));

      return new PubsubMessage(message.getPayload(), strippedAttributes);
    }
  }
}
