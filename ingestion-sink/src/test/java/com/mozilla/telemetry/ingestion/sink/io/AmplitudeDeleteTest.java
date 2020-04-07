package com.mozilla.telemetry.ingestion.sink.io;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.core.util.Json;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ForkJoinPool;
import org.junit.Test;

public class AmplitudeDeleteTest {

  @Test
  public void canSucceedWithMock() throws IOException {
    Amplitude.Client client = mock(Amplitude.Client.class);
    Amplitude.Delete output = new Amplitude.Delete(client, 0, 0, Duration.ZERO,
        ForkJoinPool.commonPool());
    HttpURLConnection conn = mock(HttpURLConnection.class);
    when(client.getConnection()).thenReturn(conn);
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    when(conn.getOutputStream()).thenReturn(body);
    when(conn.getResponseCode()).thenReturn(200);
    when(conn.getInputStream())
        .thenReturn(new ByteArrayInputStream("{}".getBytes(StandardCharsets.UTF_8)));

    output.apply(PubsubMessage.newBuilder().putAttributes("client_id", "x")
        .putAttributes("document_namespace", "telemetry").build()).join();

    assertEquals(
        ImmutableMap.of("requester", "shredder", "ignore_invalid_id", "True", "delete_from_org",
            "False", "user_ids", ImmutableList.of("x")),
        Json.asMap(Json.readObjectNode(body.toByteArray())));
  }

  @Test
  public void canSucceedOnLocalhost() {
    // TODO test with a local http server
  }
}
