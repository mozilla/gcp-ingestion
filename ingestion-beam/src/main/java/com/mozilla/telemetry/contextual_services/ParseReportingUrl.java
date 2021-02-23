package com.mozilla.telemetry.contextual_services;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.Json;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Extract reporting URL from document and filter out unknown URLs
 */
public class ParseReportingUrl extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  public static ParseReportingUrl of(ValueProvider<String> urlAllowList) {
    return new ParseReportingUrl();
  }

  private static class UnrecognizedUrlException extends RuntimeException {
    UnrecognizedUrlException(String message) {
      super(message);
    }
  }

  @Override
  public Result<PCollection<PubsubMessage>, PubsubMessage> expand(PCollection<PubsubMessage> messages) {
    return messages.apply(MapElements.into(TypeDescriptor.of(PubsubMessage.class))
      .via((PubsubMessage message) -> {
        message = PubsubConstraints.ensureNonNull(message);

        Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

        ObjectNode json;
        try {
          json = Json.readObjectNode(message.getPayload());
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }

        String reportingUrl = json.path("reporting_url").textValue();

        // TODO: check against allow list

        // TODO: add url params

        attributes.put(Attribute.REPORTING_URL, reportingUrl);

        return new PubsubMessage(message.getPayload(), attributes);
      }).exceptionsInto(TypeDescriptor.of(PubsubMessage.class))
        .exceptionsVia((WithFailures.ExceptionElement<PubsubMessage> ee) -> {
          try {
            throw ee.exception();
          } catch (UncheckedIOException e) {
            return FailureMessage.of(ParseReportingUrl.class.getSimpleName(),
                ee.element(), ee.exception());
          }
        })
    );
  }
}
