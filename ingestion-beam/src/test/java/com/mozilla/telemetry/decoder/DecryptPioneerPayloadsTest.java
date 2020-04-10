package com.mozilla.telemetry.decoder;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;

public class DecryptPioneerPayloadsTest extends TestWithDeterministicJson {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  private List<String> readTestFiles(List<String> filenames) {
    return Arrays.asList(filenames.stream().map(Resources::getResource).map(url -> {
      try {
        return Resources.toString(url, Charsets.UTF_8);
      } catch (Exception e) {
        return null;
      }
    }).toArray(String[]::new));
  }

  @Test
  public void testOutput() {
    // minimal test for throughput of a single document
    ValueProvider<String> keysLocation = pipeline
        .newProvider(Resources.getResource("pioneer/id_rsa_0.private.json").getPath());
    final List<String> input = readTestFiles(Arrays.asList("pioneer/sample.cleartext.json"));

    Result<PCollection<PubsubMessage>, PubsubMessage> output = pipeline
        .apply(Create.of(input)).apply(
            InputFileFormat.text.decode())
        .apply("AddAttributes",
            MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                .via(
                    element -> new PubsubMessage(element.getPayload(),
                        ImmutableMap.of("document_namespace", "pioneer", "document_type", "test",
                            "document_version", "1"))))
        .apply(DecryptPioneerPayloads.of(keysLocation));

    final List<String> expectedMain = input;
    final PCollection<String> main = output.output().apply("encodeTextMain",
        OutputFileFormat.text.encode());
    PAssert.that(main).containsInAnyOrder(expectedMain);

    pipeline.run();
  }
}
