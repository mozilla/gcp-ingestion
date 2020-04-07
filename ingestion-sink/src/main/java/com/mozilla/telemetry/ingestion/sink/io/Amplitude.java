package com.mozilla.telemetry.ingestion.sink.io;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.ingestion.core.util.Json;
import com.mozilla.telemetry.ingestion.sink.util.BatchWrite;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class Amplitude {

  public static class Client {

    private final URL url;
    private final String basicAuth;

    @VisibleForTesting
    Client(String url, String username, String password) {
      try {
        this.url = new URL(url);
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(e);
      }
      this.basicAuth = "Basic " + Base64.getEncoder()
          .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
    }

    public Client(String username, String password) {
      this("https://amplitude.com/api/2/deletions/users", username, password);
    }

    @VisibleForTesting
    HttpURLConnection getConnection() throws IOException {
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestProperty("Authorization", basicAuth);
      return conn;
    }
  }

  public static class Delete extends BatchWrite<PubsubMessage, String, String, Void> {

    // TODO this should be an amplitude service provider, so that tests can mock requests
    private final Client client;

    /** Constructor. */
    public Delete(Client client, long maxBytes, int maxMessages, Duration maxDelay,
        Executor executor) {
      super(maxBytes, maxMessages, maxDelay, null, executor);
      this.client = client;
    }

    @Override
    protected String encodeInput(PubsubMessage input) {
      return input.getAttributesOrThrow(Attribute.CLIENT_ID);
    }

    @Override
    protected String getBatchKey(PubsubMessage input) {
      return ""; // this is ignored, and null is not a valid batch key
    }

    @Override
    protected Batch getBatch(String ignore) {
      return new Batch();
    }

    @VisibleForTesting
    class Batch extends BatchWrite<PubsubMessage, String, String, Void>.Batch {

      private final ArrayNode userIds;

      private Batch() {
        super();
        this.userIds = Json.createArrayNode();
      }

      @Override
      protected CompletableFuture<Void> close() {
        // https://help.amplitude.com/hc/en-us/articles/360000398191-User-Privacy-API#h_5be848c6-64b7-4ae4-ad6d-a784b05dcb8b
        ObjectNode body = Json.createObjectNode().put("requester", "shredder")
            // skip invalid ID because not every deletion request will have already uploaded events
            .put("ignore_invalid_id", "True")
            // don't delete from multiple projects. If False then response will be a json object, if
            // True then response will be a json array of objects
            .put("delete_from_org", "False");
        body.set("user_ids", userIds);
        try {
          HttpURLConnection conn = client.getConnection();
          conn.setRequestProperty("Content-Type", "application/json");
          conn.setRequestMethod("POST");
          conn.setDoOutput(true);
          conn.setDoInput(true);
          try (OutputStream os = conn.getOutputStream()) {
            os.write(Json.asBytes(body));
          }
          assert conn.getResponseCode() == 200;
          try (InputStream stream = conn.getInputStream()) {
            // verify that the response was JSON and don't validate the number of IDs returned,
            // because not every deletion request is "valid" (i.e. has already uploaded events)
            Json.readJsonNode(stream);
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return CompletableFuture.completedFuture(null);
      }

      @Override
      protected void write(String userId) {
        userIds.add(userId);
      }

      @Override
      protected long getByteSize(String userId) {
        return userId.length();
      }
    }
  }
}
