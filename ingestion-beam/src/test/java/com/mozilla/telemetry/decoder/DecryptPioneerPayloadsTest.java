package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.transforms.DecompressPayload;
import com.mozilla.telemetry.util.GzipUtil;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.jose4j.jwk.PublicJsonWebKey;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class DecryptPioneerPayloadsTest extends TestWithDeterministicJson {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  private List<String> readTestFiles(List<String> filenames) {
    return Arrays.asList(filenames.stream().map(Resources::getResource).map(url -> {
      try {
        // format for comparison in testing
        String data = Resources.toString(url, Charsets.UTF_8);
        ObjectNode json = Json.readObjectNode(data);
        return json.toString();
      } catch (Exception e) {
        return null;
      }
    }).toArray(String[]::new));
  }

  private static final class ReformatJson
      extends PTransform<PCollection<String>, PCollection<String>> {

    public static ReformatJson of() {
      return new ReformatJson();
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
      return input.apply(MapElements.into(TypeDescriptors.strings()).via(s -> {
        try {
          return Json.readObjectNode(s).toString();
        } catch (Exception e) {
          return null;
        }
      }));
    }
  }

  /** Load a private key from a JWK. See the KeyStore for more details. */
  private PrivateKey loadPrivateKey(String resourceLocation) throws Exception {
    byte[] data = Resources.toByteArray(Resources.getResource(resourceLocation));
    PublicJsonWebKey key = PublicJsonWebKey.Factory.newPublicJwk(new String(data));
    return key.getPrivateKey();
  }

  @Test
  public void testDecrypt() throws Exception {
    PrivateKey key = loadPrivateKey("pioneer/study-foo.private.json");
    byte[] ciphertext = Resources
        .toByteArray(Resources.getResource("pioneer/study-foo.ciphertext.json"));
    byte[] plaintext = Resources
        .toByteArray(Resources.getResource("pioneer/sample.plaintext.json"));

    String payload = Json.readObjectNode(ciphertext).get("payload").textValue();
    byte[] decrypted = DecryptPioneerPayloads.decrypt(key, payload);
    byte[] decompressed = GzipUtil.maybeDecompress(decrypted);

    String actual = Json.readObjectNode(plaintext).toString();
    String expect = Json.readObjectNode(decompressed).toString();
    Assert.assertEquals(expect, actual);
  }

  @Test
  public void testOutput() {
    // minimal test for throughput of a single document
    ValueProvider<String> metadataLocation = pipeline
        .newProvider(Resources.getResource("pioneer/metadata-local.json").getPath());
    ValueProvider<Boolean> kmsEnabled = pipeline.newProvider(false);
    final List<String> input = readTestFiles(Arrays.asList("pioneer/study-foo.ciphertext.json"));

    PCollection<String> output = pipeline.apply(Create.of(input))
        .apply(InputFileFormat.text.decode())
        .apply("AddAttributes",
            MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                .via(element -> new PubsubMessage(element.getPayload(),
                    ImmutableMap.of("document_namespace", "study-foo", "document_type", "test",
                        "document_version", "1"))))
        .apply(DecryptPioneerPayloads.of(metadataLocation, kmsEnabled)).output()
        .apply(DecompressPayload.enabled(pipeline.newProvider(true)))
        .apply(OutputFileFormat.text.encode()).apply(ReformatJson.of());

    final List<String> expectedMain = readTestFiles(Arrays.asList("pioneer/sample.plaintext.json"));
    PAssert.that(output).containsInAnyOrder(expectedMain);

    pipeline.run();
  }
}
