package com.mozilla.telemetry.decoder.ipprivacy;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

/**
 * IP parsing code copied from GeoCityLookup.
 */
public class ParseIp extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  private static final Fn fn = new Fn();
  private static final ParseIp INSTANCE = new ParseIp();

  public static ParseIp of() {
    return INSTANCE;
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply((MapElements.via(fn)));
  }

  private static class Fn extends SimpleFunction<PubsubMessage, PubsubMessage> {

    @Override
    public PubsubMessage apply(PubsubMessage message) {
      Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

      String ip;
      String xff = attributes.get(Attribute.X_FORWARDED_FOR);
      if (xff != null) {
        // Google's load balancer will append the immediate sending client IP and a global
        // forwarding rule IP to any existing content in X-Forwarded-For as documented in:
        // https://cloud.google.com/load-balancing/docs/https/#components
        //
        // In practice, many of the "first" addresses are bogus or internal,
        // so we target the immediate sending client IP.
        String[] ips = xff.split("\\s*,\\s*");
        ip = ips[Math.max(ips.length - 2, 0)];
      } else {
        ip = attributes.getOrDefault(Attribute.REMOTE_ADDR, "");
      }
      attributes.put(Attribute.CLIENT_IP, ip);
      return new PubsubMessage(message.getPayload(), attributes);
    }
  }
}
