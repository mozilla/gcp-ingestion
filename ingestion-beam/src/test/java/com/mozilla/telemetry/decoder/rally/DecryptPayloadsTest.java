package com.mozilla.telemetry.decoder.rally;

import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class DecryptPayloadsTest extends TestWithDeterministicJson {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  private PubsubMessage doc(String namespace, String doctype) {
    return new PubsubMessage("{}".getBytes(StandardCharsets.UTF_8), //
        ImmutableMap.of(Attribute.DOCUMENT_NAMESPACE, namespace, Attribute.DOCUMENT_TYPE, doctype,
            Attribute.DOCUMENT_VERSION, "1"));
  }

  @Test
  public void testFilterPioneer() throws Exception {

    PCollection<String> result = pipeline
        .apply(Create.of(Arrays.asList(doc("telemetry", "pioneer-study"),
            doc("telemetry", "pioneer-study"), doc("telemetry", "main"))))
        .apply(Filter.by(new DecryptPayloads.PioneerPredicate()))
        .apply(OutputFileFormat.text.encode());

    PAssert.that(result).containsInAnyOrder(Arrays.asList("{}", "{}"));
    pipeline.run();
  }

  @Test
  public void testFilterRally() throws Exception {

    PCollection<String> result = pipeline
        .apply(Create.of(Arrays.asList(doc("rally", "baseline"), doc("telemetry", "baseline"),
            doc("telemetry", "pioneer-study"))))
        .apply(Filter.by(new DecryptPayloads.RallyPredicate()))
        .apply(OutputFileFormat.text.encode());

    PAssert.that(result).containsInAnyOrder(Arrays.asList("{}", "{}"));
    pipeline.run();
  }
}
