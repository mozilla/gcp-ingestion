package com.mozilla.telemetry.decoder.rally;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.ProcessFunction;

public class DecryptPayloads {

  private static Boolean isPioneerPing(PubsubMessage message) {
    message = PubsubConstraints.ensureNonNull(message);
    Map<String, String> attributes = new HashMap<>(message.getAttributeMap());
    final String namespace = attributes.get(Attribute.DOCUMENT_NAMESPACE);
    final String docType = attributes.get(Attribute.DOCUMENT_TYPE);
    return namespace.equals("telemetry") && docType.equals("pioneer-study");
  }

  public static class PioneerPredicate implements ProcessFunction<PubsubMessage, Boolean> {

    @Override
    public Boolean apply(PubsubMessage message) {
      return isPioneerPing(message);
    }
  }

  public static class RallyPredicate implements ProcessFunction<PubsubMessage, Boolean> {

    @Override
    public Boolean apply(PubsubMessage message) {
      return !isPioneerPing(message);
    }
  }
}
