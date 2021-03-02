package com.mozilla.telemetry.contextualservices;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.metrics.KeyedCounter;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithFailures.ExceptionElement;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Send GET requests to reporting endpoint.
 */
public class SendRequest extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  private static class HttpRequestException extends RuntimeException {

    HttpRequestException(String message, int code) {
      super("HTTP " + code + ": " + message);
    }
  }

  private static OkHttpClient httpClient;

  public static SendRequest of() {
    return new SendRequest();
  }

  @Override
  public Result<PCollection<PubsubMessage>, PubsubMessage> expand(
      PCollection<PubsubMessage> messages) {
    return messages.apply(
        MapElements.into(TypeDescriptor.of(PubsubMessage.class)).via((PubsubMessage message) -> {
          message = PubsubConstraints.ensureNonNull(message);

          String reportingUrl = message.getAttribute(Attribute.REPORTING_URL);

          if (reportingUrl == null) {
            throw new IllegalArgumentException("reporting url cannot be null");
          }

          getOrCreateHttpClient();

          Request request = new Request.Builder().url(reportingUrl).build();

          // TODO: first iteration of job does not retry requests on errors
          try (Response response = httpClient.newCall(request).execute()) {
            KeyedCounter.inc("reporting_response_" + response.code());
            if (!response.isSuccessful()) {
              throw new HttpRequestException(response.message(), response.code());
            }
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }

          // TODO: this output is just for testing
          return new PubsubMessage(message.getPayload(), new HashMap<>());
        }).exceptionsInto(TypeDescriptor.of(PubsubMessage.class))
            .exceptionsVia((ExceptionElement<PubsubMessage> ee) -> {
              try {
                throw ee.exception();
              } catch (UncheckedIOException e) {
                return FailureMessage.of(ParseReportingUrl.class.getSimpleName(), ee.element(),
                    ee.exception());
              }
            }));
  }

  private OkHttpClient getOrCreateHttpClient() {
    if (httpClient == null) {
      httpClient = new OkHttpClient();
    }
    return httpClient;
  }
}
