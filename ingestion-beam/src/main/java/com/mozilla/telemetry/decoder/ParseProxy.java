package com.mozilla.telemetry.decoder;

import com.google.common.annotations.VisibleForTesting;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.Time;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

public class ParseProxy extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  public static ParseProxy of(String proxyIpAddress) {
    return new ParseProxy(proxyIpAddress);
  }

  /////////

  private final String proxyIpAddress;

  @VisibleForTesting
  public class Fn extends SimpleFunction<PubsubMessage, PubsubMessage> {

    private final Counter countPipelineProxy = Metrics.counter(Fn.class, "pipeline_proxy");
    private final Counter countProxyIpAddress = Metrics.counter(Fn.class, "proxy_ip_address");
    private final Distribution teeLatencyTimer = Metrics.distribution(Fn.class,
        "tee_latency_millis");

    @Override
    public PubsubMessage apply(PubsubMessage message) {
      // Prevent null pointer exception
      message = PubsubConstraints.ensureNonNull(message);

      // Copy attributes
      Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

      final String xpp = attributes.get(Attribute.X_PIPELINE_PROXY);
      final String xff = attributes.get(Attribute.X_FORWARDED_FOR);

      if (xpp != null) {
        // If the X-Pipeline-Proxy header is present, this message came from the AWS tee.

        // Check if X-Pipeline-Proxy is a timestamp
        final Instant proxyInstant = Time.parseAsInstantOrNull(xpp);
        if (proxyInstant != null) {
          // Record the difference between submission and proxy times as tee latency.
          final String submissionTimestamp = attributes.get(Attribute.SUBMISSION_TIMESTAMP);
          final Instant submissionInstant = Time.parseAsInstantOrNull(submissionTimestamp);
          if (submissionInstant != null) {
            teeLatencyTimer.update(submissionInstant.toEpochMilli() - proxyInstant.toEpochMilli());
          }
          // Rename submission timestamp to proxy timestamp
          attributes.put(Attribute.PROXY_TIMESTAMP, submissionTimestamp);
          // Use submission timestamp from X-Pipeline-Proxy
          attributes.put(Attribute.SUBMISSION_TIMESTAMP, xpp);
        }

        // Remove the proxy attribute
        attributes.remove(Attribute.X_PIPELINE_PROXY);

        // Report proxied message
        countPipelineProxy.inc();
      } else if (xff != null && xff.contains(proxyIpAddress)) {
        // If the configured proxyIpAddress is present, then there's a GCP load balancer IP
        // we will need to remove.
        countProxyIpAddress.inc();
      } else {
        // Early return; no proxy was detected, so nothing to do here.
        return message;
      }

      // If we've reached this code, it means there's a static IP entry we need to remove from
      // X-Forwarded-For in order for IP parsing logic in downstream transforms to be correct.
      if (xff != null) {
        // Strip the last entry from X-Forwarded-For.
        attributes.put(Attribute.X_FORWARDED_FOR,
            xff.substring(0, Math.max(xff.lastIndexOf(","), 0)));
      }

      // Remove null attributes because the coder can't handle them.
      attributes.values().removeIf(Objects::isNull);

      // Return new message.
      return new PubsubMessage(message.getPayload(), attributes);
    }
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(MapElements.via(new Fn()));
  }

  private ParseProxy(String proxyIpAddress) {
    this.proxyIpAddress = proxyIpAddress;
  }

}
