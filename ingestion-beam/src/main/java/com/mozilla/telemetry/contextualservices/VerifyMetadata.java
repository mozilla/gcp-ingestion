package com.mozilla.telemetry.contextualservices;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithFailures.ExceptionElement;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Verify attributes and payload are in the expected formats.
 */
public class VerifyMetadata extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  private final ValueProvider<List<String>> docTypesToVerify;

  public static VerifyMetadata of(ValueProvider<List<String>> docTypesToCheck) {
    return new VerifyMetadata(docTypesToCheck);
  }

  private VerifyMetadata(ValueProvider<List<String>> docTypesToVerify) {
    this.docTypesToVerify = docTypesToVerify;
  }

  @Override
  public Result<PCollection<PubsubMessage>, PubsubMessage> expand(
      PCollection<PubsubMessage> messages) {
    return messages.apply(
        MapElements.into(TypeDescriptor.of(PubsubMessage.class)).via((PubsubMessage message) -> {
          message = PubsubConstraints.ensureNonNull(message);

          Map<String, String> attributes = message.getAttributeMap();

          if (!docTypesToVerify.isAccessible()
              || !docTypesToVerify.get().contains(attributes.get(Attribute.DOCUMENT_TYPE))) {
            return message;
          }

          // Message must be gzip compressed
          String clientCompression = attributes.get(Attribute.CLIENT_COMPRESSION);
          if (!"gzip".equals(clientCompression)) {
            throw new RejectedMessageException(
                String.format("Payload must be gzip compressed, found: %s",
                    Optional.ofNullable(clientCompression).orElse("none")),
                "gzip");
          }

          // User agent must be Firefox
          String userAgent = attributes.get(Attribute.USER_AGENT_BROWSER);
          if (!"Firefox".equals(userAgent)) {
            throw new RejectedMessageException("Invalid user agent: " + userAgent, "user_agent");
          }

          // Verify Firefox version
          String doctype = attributes.get(Attribute.DOCUMENT_TYPE);
          int minVersion;
          if (doctype.startsWith("topsites-")) {
            minVersion = 87;
          } else if (doctype.startsWith("quicksuggest-")) {
            minVersion = 89;
          } else {
            throw new IllegalArgumentException("Unrecognized doctype: " + doctype);
          }
          String version = attributes.get(Attribute.USER_AGENT_VERSION);
          try {
            if (version == null || minVersion > Integer.parseInt(version)) {
              throw new RejectedMessageException(
                  String.format("Firefox version does not match doctype: %s, %s", version, doctype),
                  "user_agent_version");
            }
          } catch (NumberFormatException e) {
            throw new RejectedMessageException(
                String.format("Invalid Firefox version: %s", version), "user_agent_version");
          }

          return message;
        }).exceptionsInto(TypeDescriptor.of(PubsubMessage.class))
            .exceptionsVia((ExceptionElement<PubsubMessage> ee) -> {
              try {
                throw ee.exception();
              } catch (RejectedMessageException | IllegalArgumentException e) {
                return FailureMessage.of(VerifyMetadata.class.getSimpleName(), ee.element(),
                    ee.exception());
              }
            }));
  }
}
