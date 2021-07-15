package com.mozilla.telemetry.decoder.rally;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class DecryptPayloadsTest extends TestWithDeterministicJson {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  private String readTestFile(String filename) {
    try {
      URL url = Resources.getResource(filename);
      return Json.readObjectNode(Resources.toByteArray(url)).toString();
    } catch (Exception e) {
      return null;
    }
  }

  private PubsubMessage readTestFilePubsub(String filename, String namespace, String doctype,
      String version) {
    try {
      URL url = Resources.getResource(filename);
      byte[] data = Resources.toByteArray(url);
      return new PubsubMessage(data, ImmutableMap.of(Attribute.DOCUMENT_NAMESPACE, namespace,
          Attribute.DOCUMENT_TYPE, doctype, Attribute.DOCUMENT_VERSION, version));
    } catch (Exception e) {
      return null;
    }

  }

  @Test
  public void testSuccess() throws Exception {
    ValueProvider<String> metadataLocation = pipeline
        .newProvider(Resources.getResource("jwe/metadata-local.json").getPath());
    ValueProvider<String> schemasLocation = pipeline.newProvider("schemas.tar.gz");
    ValueProvider<Boolean> kmsEnabled = pipeline.newProvider(false);
    ValueProvider<Boolean> decompressPayload = pipeline.newProvider(true);

    List<PubsubMessage> input = Arrays.asList(
        readTestFilePubsub("jwe/rally-study-foo.ciphertext.json", "rally-study-foo", "baseline",
            "1"),
        readTestFilePubsub("pioneer/study-foo.ciphertext.json", "telemetry", "pioneer-study", "4"),
        readTestFilePubsub("jwe/rally-study-foo.deletion-request.ciphertext.json",
            "rally-study-foo", "deletion-request", "1"));
    List<String> expected = Arrays.asList(readTestFile("jwe/rally-study-foo.plaintext.json"),
        readTestFile("pioneer/sample.plaintext.json"),
        readTestFile("jwe/rally-study-foo.deletion-request.plaintext.json"));
    Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline.apply(Create.of(input))
        .apply(
            DecryptPayloads.of(metadataLocation, schemasLocation, kmsEnabled, decompressPayload));

    PAssert.that(result.failures()).empty();
    pipeline.run();

    PCollection<String> output = result.output().apply(OutputFileFormat.text.encode())
        .apply(DecryptRallyPayloadsTest.ReformatJson.of());

    PAssert.that(output).containsInAnyOrder(expected);
    pipeline.run();
  }
}
