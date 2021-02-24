package com.mozilla.telemetry.contextual_services;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithFailures.ExceptionElement;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.UncheckedIOException;
import java.util.Map;

/**
 * Send GET requests to reporting
 */
public class SendRequest extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  public static SendRequest of() {
    return new SendRequest();
  }

  @Override
  public Result<PCollection<PubsubMessage>, PubsubMessage> expand(PCollection<PubsubMessage> messages) {
    return messages.apply(MapElements.into(TypeDescriptor.of(PubsubMessage.class))
        .via((PubsubMessage message) -> {
          message = PubsubConstraints.ensureNonNull(message);

          // TODO: HTTP GET
          System.out.println(message.getAttribute(Attribute.REPORTING_URL));

          return message;
        }).exceptionsInto(TypeDescriptor.of(PubsubMessage.class))
        .exceptionsVia((ExceptionElement<PubsubMessage> ee) -> {
          try {
            throw ee.exception();
          } catch (UncheckedIOException e) {
            return FailureMessage.of(ParseReportingUrl.class.getSimpleName(),
                ee.element(), ee.exception());
          }
        }));
  }
}
