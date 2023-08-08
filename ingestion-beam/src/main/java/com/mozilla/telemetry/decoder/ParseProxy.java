package com.mozilla.telemetry.decoder;

import com.google.common.annotations.VisibleForTesting;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.ingestion.core.schema.PipelineMetadataStore;
import com.mozilla.telemetry.ingestion.core.schema.SchemaNotFoundException;
import com.mozilla.telemetry.schema.SchemaStoreSingletonFactory;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;

public class ParseProxy extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  public static ParseProxy of(@Nullable String schemasLocation) {
    return new ParseProxy(schemasLocation);
  }

  /////////

  private final String schemasLocation;

  private transient PipelineMetadataStore metadataStore;

  private ParseProxy(@Nullable String schemasLocation) {
    this.schemasLocation = schemasLocation;
  }

  @VisibleForTesting
  class Fn extends DoFn<PubsubMessage, PubsubMessage> {

    @Setup
    public void setup() {
      if (schemasLocation != null) {
        metadataStore = SchemaStoreSingletonFactory.getPipelineMetadataStore(schemasLocation);
      }
    }

    @ProcessElement
    public void processElement(@Element PubsubMessage rawMessage,
        OutputReceiver<PubsubMessage> out) {
      // Prevent null pointer exception
      final PubsubMessage message = PubsubConstraints.ensureNonNull(rawMessage);

      // Copy attributes
      final Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

      Optional.ofNullable(attributes.get(Attribute.X_FORWARDED_FOR))
          .map(v -> Arrays.stream(v.split("\\s*,\\s*")).filter(StringUtils::isNotBlank)
              .collect(Collectors.toList()))
          .ifPresent(xff -> {
            // Google's load balancer will append the immediate sending client IP and a global
            // forwarding rule IP to any existing content in X-Forwarded-For as documented in:
            // https://cloud.google.com/load-balancing/docs/https/#components
            //
            // Our nginx setup will then append google's load balancer IP.
            //
            // In practice, many of the "first" addresses are bogus or internal, so remove load
            // balancer entries and downstream transforms will read the immediate sending client
            // IP from the end of the list.
            if (xff.size() > 1) {
              // Remove load balancer IP
              xff.remove(xff.size() - 1);
              // Remove global forwarding rule IP
              xff.remove(xff.size() - 1);
            }

            if (schemasLocation != null) {
              // Remove trailing entries as indicated by metadata
              try {
                final Integer geoipSkipEntries = metadataStore.getSchema(attributes)
                    .geoip_skip_entries();
                if (geoipSkipEntries != null) {
                  for (int i = 0; i < geoipSkipEntries; i++) {
                    xff.remove(xff.size() - 1);
                  }
                }
              } catch (SchemaNotFoundException ignore) {
                // this function is not allowed to fail, so ignore the lack of schema
              }
            }

            attributes.put(Attribute.X_FORWARDED_FOR, String.join(",", xff));
          });

      // Remove unused ip from attributes
      attributes.remove(Attribute.REMOTE_ADDR);

      // Remove null attributes because the coder can't handle them.
      attributes.values().removeIf(Objects::isNull);

      // Return new message.
      out.output(new PubsubMessage(message.getPayload(), attributes));
    }
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(ParDo.of(new Fn()));
  }
}
