package com.mozilla.telemetry.decoder;

import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;

public class PioneerDecryptorTest extends TestWithDeterministicJson {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testOutput() {
    // minimal test for throughput of a single document
    final List<String> input = Arrays.asList("{}");
    Result<PCollection<PubsubMessage>, PubsubMessage> output = pipeline
        .apply(Create.of(input)).apply(
            InputFileFormat.text.decode())
        .apply("AddAttributes", MapElements.into(TypeDescriptor.of(PubsubMessage.class)).via(
            element -> new PubsubMessage(element.getPayload(), ImmutableMap.of("document_namespace",
                "pioneer", "document_type", "test", "document_version", "1"))))
        .apply(PioneerDecryptor.of());

    final List<String> expectedMain = Arrays.asList("{}");
    final PCollection<String> main = output.output().apply("encodeTextMain",
        OutputFileFormat.text.encode());
    PAssert.that(main).containsInAnyOrder(expectedMain);

    pipeline.run();
  }
}
