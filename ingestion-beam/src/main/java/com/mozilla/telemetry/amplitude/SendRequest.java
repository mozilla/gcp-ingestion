package com.mozilla.telemetry.amplitude;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.metrics.KeyedCounter;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.io.UncheckedIOException;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithFailures.ExceptionElement;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Send GET requests to reporting endpoint.
 */
@SuppressWarnings("checkstyle:lineLength")
public class SendRequest extends
    PTransform<PCollection<AmplitudeEvent>, Result<PCollection<AmplitudeEvent>, PubsubMessage>> {

  private static OkHttpClient httpClient;

  private final Distribution requestTimer = Metrics.distribution(SendRequest.class,
      "reporting_request_millis");
  private final Boolean reportingEnabled;

  public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

  private static class HttpRequestException extends IOException {

    HttpRequestException(String message, int code) {
      super("HTTP " + code + ": " + message);
    }
  }

  private static class HttpRedirectException extends IOException {

    HttpRedirectException(String url) {
      super("Redirects not allowed: " + url);
    }
  }

  /**
   * Used to log requests for debugging purposes.
   */
  @VisibleForTesting
  static class RequestContentException extends RuntimeException {

    RequestContentException(String url) {
      super("Reporting URL sent: " + url);
    }
  }

  public SendRequest(Boolean reportingEnabled) {
    this.reportingEnabled = reportingEnabled;
  }

  public static SendRequest of(Boolean reportingEnabled) {
    return new SendRequest(reportingEnabled);
  }

  @Override
  public Result<PCollection<AmplitudeEvent>, PubsubMessage> expand(
      PCollection<AmplitudeEvent> events) {
    return events.apply(
        MapElements.into(TypeDescriptor.of(AmplitudeEvent.class)).via((AmplitudeEvent event) -> {

          getOrCreateHttpClient();

          ObjectNode body = Json.createObjectNode();
          body.put("api_key", Attribute.AMPLITUDE_API_KEY);

          try {
            ArrayNode jsonEvents = Json.createArrayNode();
            jsonEvents.add(event.toJson());
            body.put("events", jsonEvents);
          } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
          }

          RequestBody requestBody = RequestBody.create(body.toString(), JSON);
          Request request = new Request.Builder().url("https://api2.amplitude.com/batch")
              .post(requestBody).build();

          if (reportingEnabled) {
            sendRequest(request);
          }

          return event;
        }).exceptionsInto(TypeDescriptor.of(PubsubMessage.class))
            .exceptionsVia((ExceptionElement<AmplitudeEvent> ee) -> {
              try {
                throw ee.exception();
              } catch (UncheckedIOException e) {
                return FailureMessage.of(SendRequest.class.getSimpleName(), ee.element(),
                    ee.exception());
              }
            }));
  }

  private void sendRequest(Request request) {
    try (Response response = httpClient.newCall(request).execute()) {
      KeyedCounter.inc("reporting_response_" + response.code());

      long requestDuration = response.receivedResponseAtMillis() - response.sentRequestAtMillis();
      requestTimer.update(requestDuration);

      // TODO: first iteration of job does not retry requests on errors
      if (response.isRedirect()) {
        // Get url without query params
        HttpUrl url = request.url();
        throw new HttpRedirectException(url.host() + url.encodedPath());
      } else if (!response.isSuccessful()) {
        throw new HttpRequestException(response.message(), response.code());
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private OkHttpClient getOrCreateHttpClient() {
    if (httpClient == null) {
      httpClient = new OkHttpClient.Builder().followRedirects(false).build();
    }
    return httpClient;
  }

}
