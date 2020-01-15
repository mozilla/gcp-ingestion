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

  public static ParseProxy of() {
    return INSTANCE;
  }

  /////////

  private static final Fn FN = new Fn();
  private static final ParseProxy INSTANCE = new ParseProxy();

  @VisibleForTesting
  public static class Fn extends SimpleFunction<PubsubMessage, PubsubMessage> {

    private final Counter countPipelineProxy = Metrics.counter(Fn.class, "pipeline_proxy");
    private final Distribution teeLatencyTimer = Metrics.distribution(Fn.class,
        "tee_latency_millis");

    @Override
    public PubsubMessage apply(PubsubMessage message) {
      // Prevent null pointer exception
      message = PubsubConstraints.ensureNonNull(message);

      // Copy attributes
      Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

      String xpp = attributes.get(Attribute.X_PIPELINE_PROXY);
      if (xpp != null) {

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

        // Drop extra IP from X-Forwarded-For
        String xff = attributes.get(Attribute.X_FORWARDED_FOR);
        if (xff != null) {
          attributes.put(Attribute.X_FORWARDED_FOR,
              xff.substring(0, Math.max(xff.lastIndexOf(","), 0)));
        }

        // Remove the proxy attribute
        attributes.remove(Attribute.X_PIPELINE_PROXY);

        // remove null attributes because the coder can't handle them
        attributes.values().removeIf(Objects::isNull);

        // Report proxied message
        countPipelineProxy.inc();
      }

      // Return new message
      return new PubsubMessage(message.getPayload(), attributes, message.getMessageId());
    }
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(MapElements.via(FN));
  }

  private ParseProxy() {
  }

}
