package com.mozilla.telemetry.ingestion.sink.io;

import com.google.cloud.bigquery.TableId;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.core.transform.PubsubMessageToObjectNode;
import com.mozilla.telemetry.ingestion.core.util.Json;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToTemplatedString;
import com.mozilla.telemetry.ingestion.sink.transform.StringToPubsubMessage;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class Pipe {

  /** Never instantiate this class. */
  private Pipe() {
  }

  public static class Read<ResultT> implements Input {

    private final InputStream pipe;
    private final Function<PubsubMessage, CompletableFuture<ResultT>> output;
    private final Function<PubsubMessage, PubsubMessage> decompress;

    private Read(InputStream pipe, Function<PubsubMessage, CompletableFuture<ResultT>> output,
        Function<PubsubMessage, PubsubMessage> decompress) {
      this.pipe = pipe;
      this.output = output;
      this.decompress = decompress;
    }

    public static <T> Read<T> of(InputStream pipe,
        Function<PubsubMessage, CompletableFuture<T>> output,
        Function<PubsubMessage, PubsubMessage> decompress) {
      return new Read<>(pipe, output, decompress);
    }

    /** Read stdin until end of stream. */
    @Override
    public void run() {
      BufferedReader in = new BufferedReader(new InputStreamReader(pipe, StandardCharsets.UTF_8));
      in.lines().map(StringToPubsubMessage::apply).map(decompress).map(output)
          .forEach(CompletableFuture::join);
    }
  }

  public static class Write implements Function<PubsubMessage, CompletableFuture<Void>> {

    private final PrintStream pipe;
    private final PubsubMessageToObjectNode encoder;
    private final Function<PubsubMessage, TableId> tableTemplate;

    private Write(PrintStream pipe, PubsubMessageToTemplatedString tableTemplate,
        PubsubMessageToObjectNode encoder) {
      this.pipe = pipe;
      if (tableTemplate == null) {
        this.tableTemplate = m -> null;
      } else {
        this.tableTemplate = m -> BigQuery.parseTableId(tableTemplate.apply(m));
      }
      this.encoder = encoder;
    }

    public static Write of(PrintStream pipe, PubsubMessageToTemplatedString tableTemplate,
        PubsubMessageToObjectNode encoder) {
      return new Write(pipe, tableTemplate, encoder);
    }

    @Override
    public CompletableFuture<Void> apply(PubsubMessage message) {
      pipe.println(Json.asString(encoder.apply(tableTemplate.apply(message),
          message.getAttributesMap(), message.getData().toByteArray())));
      return CompletableFuture.completedFuture(null);
    }
  }
}
