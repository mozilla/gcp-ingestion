package com.mozilla.telemetry.ingestion.sink;

import com.mozilla.telemetry.ingestion.sink.util.GcsBucket;
import org.junit.Rule;
import org.junit.Test;

public class SinkBigQueryMixedIntegrationTest extends SinkBigQueryStreamingIntegrationTest {

  @Rule
  public final GcsBucket gcs = new GcsBucket();

  @Test
  @Override
  public void canSinkRawMessages() throws Exception {
    environmentVariables.set("INPUT_SUBSCRIPTION", pubsub.getSubscription(0));
    environmentVariables.set("OUTPUT_BUCKET", gcs.bucket);
    environmentVariables.set("BIG_QUERY_OUTPUT_MODE", "mixed");
    environmentVariables.set("BATCH_MAX_DELAY", "0.001s");
    environmentVariables.set("FILE_MAX_DELAY", "0.001s");
    environmentVariables.set("STREAMING_BATCH_MAX_DELAY", "0.001s");
    environmentVariables.set("STREAMING_MESSAGE_MAX_BYTES", "6");
    environmentVariables.set("BATCH_MAX_DELAY", "0.001s");
    environmentVariables.set("OUTPUT_TABLE",
        bq.project + "." + bq.dataset + ".${document_type}_v${document_version}");
    runTest();
  }
}
