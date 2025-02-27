package com.mozilla.telemetry.amplitude;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mozilla.telemetry.metrics.KeyedCounter;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Send GET requests of event batches to Amplitude endpoint.
 */
@SuppressWarnings("checkstyle:lineLength")
public class SendRequest extends
    PTransform<PCollection<KV<String, Iterable<AmplitudeEvent>>>, Result<PCollection<KV<String, Iterable<AmplitudeEvent>>>, PubsubMessage>> {

  private static OkHttpClient httpClient;

  private final Distribution requestTimer = Metrics.distribution(SendRequest.class,
      "reporting_request_millis");
  private final Boolean reportingEnabled;
  private final String amplitudeUrl;
  private final Integer maxBatchesPerSecond;
  private final Map<String, String> apiKeys;

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
   * Constructor without custom Amplitude URL.
   */
  public SendRequest(Map<String, String> apiKeys, Boolean reportingEnabled,
      Integer maxBatchesPerSecond) {
    this(apiKeys, reportingEnabled, maxBatchesPerSecond, "https://api2.amplitude.com");
  }

  /**
   * Constructor with custom Amplitude API URL.
   */
  public SendRequest(Map<String, String> apiKeys, Boolean reportingEnabled,
      Integer maxBatchesPerSecond, String amplitudeUrl) {
    this.reportingEnabled = reportingEnabled;
    this.amplitudeUrl = amplitudeUrl;
    this.maxBatchesPerSecond = maxBatchesPerSecond;
    this.apiKeys = apiKeys;
  }

  public static SendRequest of(Map<String, String> apiKeys, Boolean reportingEnabled,
      Integer maxBatchesPerSecond) {
    return new SendRequest(apiKeys, reportingEnabled, maxBatchesPerSecond);
  }

  public static SendRequest of(Map<String, String> apiKeys, Boolean reportingEnabled,
      Integer maxBatchesPerSecond, String amplitudeUrl) {
    return new SendRequest(apiKeys, reportingEnabled, maxBatchesPerSecond, amplitudeUrl);
  }

  @Override
  public Result<PCollection<KV<String, Iterable<AmplitudeEvent>>>, PubsubMessage> expand(
      PCollection<KV<String, Iterable<AmplitudeEvent>>> eventBatches) {
    TypeDescriptor<KV<String, Iterable<AmplitudeEvent>>> td = new TypeDescriptor<KV<String, Iterable<AmplitudeEvent>>>() {
    };
    return eventBatches
        .apply(MapElements.into(td).via((KV<String, Iterable<AmplitudeEvent>> eventBatch) -> {
          getOrCreateHttpClient();
          ObjectNode body = Json.createObjectNode();

          if (apiKeys.containsKey(eventBatch.getKey())) {
            body.put("api_key", this.apiKeys.get(eventBatch.getKey()));
          } else {
            throw new UncheckedIOException(
                new IOException("No API key for " + eventBatch.getKey()));
          }

          ArrayNode jsonEvents = Json.createArrayNode();

          for (AmplitudeEvent event : eventBatch.getValue()) {
            try {
              jsonEvents.add(event.toJson());
            } catch (JsonProcessingException e) {
              throw new UncheckedIOException(e);
            }
          }

          body.put("events", jsonEvents);

          RequestBody requestBody = RequestBody.create(body.toString(), JSON);
          Request request = new Request.Builder().url(amplitudeUrl + "/batch").post(requestBody)
              .build();

          if (reportingEnabled) {
            sendRequest(request);
          }

          return eventBatch;
        }).exceptionsInto(TypeDescriptor.of(PubsubMessage.class))
            .exceptionsVia((ExceptionElement<KV<String, Iterable<AmplitudeEvent>>> ee) -> {
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
