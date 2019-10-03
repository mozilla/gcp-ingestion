package com.mozilla.telemetry.ingestion.sink.transform;

import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.core.util.DerivedAttributesMap;
import java.util.function.Function;
import org.apache.commons.text.StringSubstitutor;

/**
 * Transform a {@link PubsubMessage} into a {@link String} using {@link StringSubstitutor}.
 *
 * <p>All message attributes, plus derived attributes provided by {@link DerivedAttributesMap},
 * are available for substitution.
 *
 * @throws IllegalArgumentException if template is not fully resolved for a given message.
 */
public class PubsubMessageToTemplatedString implements Function<PubsubMessage, String> {

  public final String template;

  public PubsubMessageToTemplatedString(String template) {
    this.template = template;
  }

  @Override
  public String apply(PubsubMessage message) {
    String batchKey = StringSubstitutor.replace(template,
        DerivedAttributesMap.of(message.getAttributesMap()));
    if (batchKey.contains("$")) {
      throw new IllegalArgumentException("Element did not contain all the attributes needed to"
          + " fill out variables in the configured template: " + template);
    }
    return batchKey;
  }
}
