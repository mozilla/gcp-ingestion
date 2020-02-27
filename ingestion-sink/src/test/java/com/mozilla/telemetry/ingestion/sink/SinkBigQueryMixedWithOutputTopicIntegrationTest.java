package com.mozilla.telemetry.ingestion.sink;

import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.BoundedSink;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;

public class SinkBigQueryMixedWithOutputTopicIntegrationTest
    extends SinkBigQueryMixedIntegrationTest {

  protected int numTopics() {
    return 2;
  }

  @Override
  protected List<PubsubMessage> getInputs() {
    // Only use oversize messages so BoundedSink is called with a correct messageCount in runTest()
    return super.getInputs().subList(1, 2);
  }

  @Override
  protected List<List<String>> getExpected() {
    return super.getExpected().subList(1, 2);
  }

  @Test
  @Override
  public void canSinkMessages() throws Exception {
    environmentVariables.set("BATCH_MAX_DELAY", "0.001s");
    environmentVariables.set("BIG_QUERY_OUTPUT_MODE", "mixed");
    environmentVariables.set("INPUT_SUBSCRIPTION", pubsub.getSubscription(0));
    environmentVariables.set("OUTPUT_BUCKET", gcs.bucket);
    environmentVariables.set("OUTPUT_TABLE",
        bq.project + "." + bq.dataset + ".${document_type}_v${document_version}");
    environmentVariables.set("OUTPUT_TOPIC", pubsub.getTopic(1));
    environmentVariables.set("STREAMING_BATCH_MAX_DELAY", "0.001s");
    final CompletableFuture<Void> main = BoundedSink.runAsync(inputs.size());
    // unregister OpenCensus stackdriver exporter so runTest() below can register a new one
    StackdriverStatsExporter.unregister();

    environmentVariables.clear("BATCH_MAX_DELAY", "BIG_QUERY_OUTPUT_MODE", "OUTPUT_BUCKET",
        "OUTPUT_TABLE", "OUTPUT_TOPIC", "STREAMING_BATCH_MAX_DELAY");
    environmentVariables.set("INPUT_SUBSCRIPTION", pubsub.getSubscription(1));
    environmentVariables.set("LOAD_MAX_DELAY", "0.001s");
    try {
      runTest();
    } finally {
      BoundedSink.run(main, 1);
    }
  }
}
