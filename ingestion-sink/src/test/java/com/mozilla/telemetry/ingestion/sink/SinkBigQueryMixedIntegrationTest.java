package com.mozilla.telemetry.ingestion.sink;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.GcsBucket;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;

public class SinkBigQueryMixedIntegrationTest extends SinkBigQueryStreamingIntegrationTest {

  private static final String OVERSIZE_ROW_DATA = Strings.repeat("x", 2 * 1024 * 1024);
  private static final String OVERSIZE_REQUEST_DATA = Strings.repeat("x", 8 * 1024 * 1024);

  @Rule
  public final GcsBucket gcs = new GcsBucket();

  @Override
  protected List<PubsubMessage> getInputs() {
    return ImmutableList.of(super.getInputs().get(0),
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(OVERSIZE_ROW_DATA)) //
            .putAttributes("document_type", "test") //
            .putAttributes("document_version", "2") //
            .putAttributes("submission_timestamp", SUBMISSION_TIMESTAMP) //
            .build(),
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(OVERSIZE_REQUEST_DATA)) //
            .putAttributes("document_type", "test") //
            .putAttributes("document_version", "3") //
            .putAttributes("submission_timestamp", SUBMISSION_TIMESTAMP) //
            .build());
  }

  @Override
  protected List<List<String>> getExpected() {
    return ImmutableList.of(super.getExpected().get(0),
        ImmutableList.of("test", "2", SUBMISSION_TIMESTAMP, OVERSIZE_ROW_DATA, "test_v2"),
        ImmutableList.of("test", "3", SUBMISSION_TIMESTAMP, OVERSIZE_REQUEST_DATA, "test_v3"));
  }

  @Test
  @Override
  public void canSinkMessages() throws Exception {
    environmentVariables.set("BATCH_MAX_DELAY", "0.001s");
    environmentVariables.set("BIG_QUERY_OUTPUT_MODE", "mixed");
    environmentVariables.set("INPUT_SUBSCRIPTION", pubsub.getSubscription(0));
    environmentVariables.set("LOAD_MAX_DELAY", "0.001s");
    environmentVariables.set("OUTPUT_BUCKET", gcs.bucket);
    environmentVariables.set("OUTPUT_TABLE",
        bq.project + "." + bq.dataset + ".${document_type}_v${document_version}");
    environmentVariables.set("STREAMING_BATCH_MAX_DELAY", "0.001s");
    runTest();
  }
}
