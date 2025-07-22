package com.mozilla.telemetry.posthog;

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

@SuppressWarnings("checkstyle:lineLength")
public class SendPosthogRequest extends
    PTransform<PCollection<KV<String, Iterable<PosthogEvent>>>, Result<PCollection<KV<String, Iterable<PosthogEvent>>>, PubsubMessage>> {

  private static OkHttpClient httpClient;
  private final String posthogUrl;

  private final Distribution requestTimer = Metrics.distribution(SendPosthogRequest.class,
      "reporting_request_millis");
  private final Map<String, String> apiKeys;
  private final boolean reportingEnabled;
  private final int maxBatchesPerSecond;

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
   * Constructor without custom Posthog URL.
   */
  public SendPosthogRequest(Map<String, String> apiKeys, Boolean reportingEnabled,
      Integer maxBatchesPerSecond) {
    this(apiKeys, reportingEnabled, maxBatchesPerSecond, "https://us.i.posthog.com");
  }

  private SendPosthogRequest(Map<String, String> apiKeys, boolean reportingEnabled,
      int maxBatchesPerSecond, String posthogUrl) {
    this.apiKeys = apiKeys;
    this.reportingEnabled = reportingEnabled;
    this.maxBatchesPerSecond = maxBatchesPerSecond;
    this.posthogUrl = posthogUrl;
  }

  public static SendPosthogRequest of(Map<String, String> apiKeys, boolean reportingEnabled,
      int maxBatchesPerSecond) {
    return new SendPosthogRequest(apiKeys, reportingEnabled, maxBatchesPerSecond);
  }

  public static SendPosthogRequest of(Map<String, String> apiKeys, Boolean reportingEnabled,
      Integer maxBatchesPerSecond, String posthogUrl) {
    return new SendPosthogRequest(apiKeys, reportingEnabled, maxBatchesPerSecond, posthogUrl);
  }

  @Override
  public Result<PCollection<KV<String, Iterable<PosthogEvent>>>, PubsubMessage> expand(
      PCollection<KV<String, Iterable<PosthogEvent>>> eventBatches) {
    TypeDescriptor<KV<String, Iterable<PosthogEvent>>> td = new TypeDescriptor<KV<String, Iterable<PosthogEvent>>>() {
    };
    return eventBatches
        .apply(MapElements.into(td).via((KV<String, Iterable<PosthogEvent>> eventBatch) -> {
          getOrCreateHttpClient();
          ObjectNode body = Json.createObjectNode();

          String shardedKey = eventBatch.getKey();
          String platform = shardedKey.split("::")[0];

          if (apiKeys.containsKey(platform)) {
            body.put("api_key", this.apiKeys.get(platform));
          } else {
            throw new UncheckedIOException(new IOException("No API key for " + platform));
          }

          ArrayNode jsonEvents = Json.createArrayNode();

          for (PosthogEvent event : eventBatch.getValue()) {
            jsonEvents.add(event.toJson());
          }

          body.put("batch", jsonEvents);

          RequestBody requestBody = RequestBody.create(body.toString(), JSON);
          Request request = new Request.Builder().url(posthogUrl + "/batch").post(requestBody)
              .build();

          if (reportingEnabled) {
            sendRequest(request);
          }

          return eventBatch;
        }).exceptionsInto(TypeDescriptor.of(PubsubMessage.class))
            .exceptionsVia((ExceptionElement<KV<String, Iterable<PosthogEvent>>> ee) -> {
              try {
                throw ee.exception();
              } catch (UncheckedIOException e) {
                return FailureMessage.of(SendPosthogRequest.class.getSimpleName(), ee.element(),
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
