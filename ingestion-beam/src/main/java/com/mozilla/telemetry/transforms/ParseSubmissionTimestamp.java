/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class ParseSubmissionTimestamp {

  /**
   * If enabled is set to true, returns a {@link PTransform} that parses a "submission_timestamp"
   * attribute if it exists, adding "submission_date" and "submission_hour" attributes to the
   * returned messages. If false, returns a no-op {@link PTransform} that passes messages through
   * unaltered.
   */
  public static PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> enabled(
      boolean enabled) {
    if (enabled) {
      return ENABLED;
    } else {
      return DISABLED;
    }
  }

  private static Enabled ENABLED = new Enabled();
  private static Disabled DISABLED = new Disabled();
  private static String SUBMISSION_TIMESTAMP = "submission_timestamp";

  @VisibleForTesting
  static Map<String, String> enrichedAttributes(Map<String, String> originalAttributes) {
    if (originalAttributes != null && originalAttributes.containsKey(SUBMISSION_TIMESTAMP)) {
      Map<String, String> attributes = new HashMap<>(originalAttributes);
      String timestamp = Optional.ofNullable(attributes.get(SUBMISSION_TIMESTAMP)).orElse("");
      if (timestamp.length() >= 10) {
        attributes.put("submission_date", timestamp.substring(0, 10));
      }
      if (timestamp.length() >= 13) {
        attributes.put("submission_hour", timestamp.substring(11, 13));
      }
      return attributes;
    } else {
      return originalAttributes;
    }
  }

  private static class Enabled
      extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

    @Override
    public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
      return input.apply(MapElements.into(new TypeDescriptor<PubsubMessage>() {
      }).via(message -> new PubsubMessage(message.getPayload(),
          enrichedAttributes(message.getAttributeMap()))));
    }
  }

  private static class Disabled
      extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

    @Override
    public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
      return input;
    }
  }
}
