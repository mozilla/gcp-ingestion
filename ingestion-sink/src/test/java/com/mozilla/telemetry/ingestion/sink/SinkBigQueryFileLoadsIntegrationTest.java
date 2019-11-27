package com.mozilla.telemetry.ingestion.sink;

import com.mozilla.telemetry.ingestion.sink.util.BoundedSink;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;

public class SinkBigQueryFileLoadsIntegrationTest extends SinkBigQueryMixedIntegrationTest {

  protected int numTopics() {
    return 2;
  }

  @Test
  @Override
  public void canSinkRawMessages() throws Exception {
    environmentVariables.set("BATCH_MAX_DELAY", "0.001s");
    environmentVariables.set("BIG_QUERY_OUTPUT_MODE", "file_loads");
    environmentVariables.set("INPUT_SUBSCRIPTION", pubsub.getSubscription(0));
    environmentVariables.set("OUTPUT_BUCKET", gcs.bucket);
    environmentVariables.set("OUTPUT_TABLE",
        bq.project + "." + bq.dataset + ".${document_type}_v${document_version}");
    environmentVariables.set("OUTPUT_TOPIC", pubsub.getTopic(1));
    final CompletableFuture<Void> main = BoundedSink.runAsync(inputs.size());

    environmentVariables.clear("BATCH_MAX_DELAY", "BIG_QUERY_OUTPUT_MODE", "OUTPUT_BUCKET",
        "OUTPUT_TABLE", "OUTPUT_TOPIC");
    environmentVariables.set("LOAD_MAX_DELAY", "0.001s");
    environmentVariables.set("INPUT_SUBSCRIPTION", pubsub.getSubscription(1));
    try {
      runTest();
    } finally {
      BoundedSink.run(main, 1);
    }
  }
}
