package com.mozilla.telemetry.amplitude;

import com.google.common.annotations.VisibleForTesting;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.metrics.KeyedCounter;
import com.mozilla.telemetry.transforms.FailureMessage;
import java.io.IOException;
import java.io.UncheckedIOException;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
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
import org.json.JSONObject;

/**
 * Send GET requests to reporting endpoint.
 */
@SuppressWarnings("checkstyle:lineLength")
public class SendRequest extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  private static OkHttpClient httpClient;

  private final Distribution requestTimer = Metrics.distribution(SendRequest.class,
      "reporting_request_millis");
  private final Boolean reportingEnabled;

  // public static final MediaType JSON
  // = MediaType.parse("application/json; charset=utf-8");

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
  public Result<PCollection<PubsubMessage>, PubsubMessage> expand(
      PCollection<PubsubMessage> interactions) {
    return interactions.apply(MapElements.into(TypeDescriptor.of(PubsubMessage.class))
        .via((PubsubMessage interaction) -> {

          getOrCreateHttpClient();

          JSONObject body = new JSONObject();
          body.put("api_key", Attribute.AMPLITUDE_API_KEY);
          // body.put("events", )

          Request request = new Request.Builder().url("https://api2.amplitude.com/batch").build();

          if (reportingEnabled) {
            sendRequest(request);
          }

          return interaction;
        }).exceptionsInto(TypeDescriptor.of(PubsubMessage.class))
        .exceptionsVia((ExceptionElement<PubsubMessage> ee) -> {
          try {
            throw ee.exception();
          } catch (UncheckedIOException | RequestContentException e) {
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

      // todo transform payload etc

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
