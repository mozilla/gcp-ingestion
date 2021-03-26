package com.mozilla.telemetry.decoder.rally;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.util.GzipUtil;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.jose4j.jwe.ContentEncryptionAlgorithmIdentifiers;
import org.jose4j.jwe.JsonWebEncryption;
import org.jose4j.jwe.KeyManagementAlgorithmIdentifiers;
import org.jose4j.jwk.PublicJsonWebKey;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class DecryptRallyPayloadsTest extends TestWithDeterministicJson {

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

  /** Remove metadata inserted by the rally transform and assert they existed.
   * If the do not exist, return null. */
  public static String removeMetadata(String jsonData) {
    try {
      ObjectNode node = Json.readObjectNode(jsonData);
      JsonNode pioneerId = node.remove(DecryptRallyPayloads.PIONEER_ID);
      JsonNode rallyId = node.remove(DecryptRallyPayloads.RALLY_ID);
      // one of the two must be set, and we enforce that both are not set in the
      // ingestion metadata schema
      assertTrue(pioneerId != null ^ rallyId != null);
      assertNotNull(node.remove(DecryptPioneerPayloads.STUDY_NAME));
      return node.toString();
    } catch (Exception e) {
      return null;
    }
  }

  /** Reformat the document, and remove the expected metadata. */
  private static final class ReformatJson
      extends PTransform<PCollection<String>, PCollection<String>> {

    public static ReformatJson of() {
      return new ReformatJson();
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
      return input.apply(MapElements.into(TypeDescriptors.strings()).via(s -> removeMetadata(s)));
    }
  }

  /** Load a private key from a JWK. See the KeyStore for more details. */
  private PrivateKey loadPrivateKey(String resourceLocation) throws Exception {
    return loadPublicKey(resourceLocation).getPrivateKey();
  }

  /** Load a public key from a JWK. See the KeyStore for more details. */
  private static PublicJsonWebKey loadPublicKey(String resourceLocation) throws Exception {
    byte[] data = Resources.toByteArray(Resources.getResource(resourceLocation));
    return PublicJsonWebKey.Factory.newPublicJwk(new String(data));
  }

  /** Encrypt a string using a key stored in test resources. */
  private String encryptWithTestPublicKey(String resourceLocation, String payload)
      throws Exception {
    PublicJsonWebKey key = loadPublicKey(resourceLocation);
    JsonWebEncryption jwe = new JsonWebEncryption();
    jwe.setKey(key.getKey());
    jwe.setKeyIdHeaderValue(key.getKeyId());
    jwe.setAlgorithmHeaderValue(KeyManagementAlgorithmIdentifiers.ECDH_ES_A256KW);
    jwe.setEncryptionMethodHeaderParameter(ContentEncryptionAlgorithmIdentifiers.AES_256_GCM);
    jwe.setPayload(payload);
    return jwe.getCompactSerialization();
  }

  /** Test multiple pre-encrypted objects. */
  private void testDecryptHelper(String name) throws Exception {
    PrivateKey key = loadPrivateKey(String.format("jwe/%s.private.json", name));
    byte[] ciphertext = Resources
        .toByteArray(Resources.getResource(String.format("jwe/%s.ciphertext.json", name)));
    byte[] plaintext = Resources
        .toByteArray(Resources.getResource(String.format("jwe/%s.plaintext.json", name)));

    String payload = Json.readObjectNode(ciphertext).get("payload").textValue();
    byte[] decrypted = DecryptRallyPayloads.decrypt(key, payload);
    byte[] decompressed = GzipUtil.maybeDecompress(decrypted);

    String expect = Json.readObjectNode(plaintext).toString();
    String actual = Json.readObjectNode(decompressed).toString();
    Assert.assertEquals(expect, actual);
  }

  @Test
  public void testDecryptOnlyUuidMetric() throws Exception {
    testDecryptHelper("rally-study-foo");
  }

  @Test
  public void testDecryptUuidMetricAndCounter() throws Exception {
    testDecryptHelper("rally-study-bar");
  }

  private Result<PCollection<PubsubMessage>, PubsubMessage> getPipelineResult(TestPipeline pipeline,
      List<String> input, String namespace) {
    ValueProvider<String> metadataLocation = pipeline
        .newProvider(Resources.getResource("jwe/metadata-local.json").getPath());
    ValueProvider<String> schemasLocation = pipeline.newProvider("schemas.tar.gz");
    ValueProvider<Boolean> kmsEnabled = pipeline.newProvider(false);
    ValueProvider<Boolean> decompressPayload = pipeline.newProvider(true);

    return pipeline.apply(Create.of(input)).apply(InputFileFormat.text.decode())
        .apply("AddAttributes",
            MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                .via(element -> new PubsubMessage(element.getPayload(),
                    ImmutableMap.of(Attribute.DOCUMENT_NAMESPACE, namespace,
                        Attribute.DOCUMENT_TYPE, "baseline", Attribute.DOCUMENT_VERSION, "1"))))
        .apply(DecryptRallyPayloads.of(metadataLocation, schemasLocation, kmsEnabled,
            decompressPayload));
  }

  private void testSuccess(String namespace, List<String> input, List<String> expected)
      throws Exception {
    Result<PCollection<PubsubMessage>, PubsubMessage> result = getPipelineResult(pipeline, input,
        namespace);

    PAssert.that(result.failures()).empty();
    pipeline.run();

    PCollection<String> output = result.output().apply(OutputFileFormat.text.encode())
        .apply(ReformatJson.of());

    PAssert.that(output).containsInAnyOrder(expected);
    pipeline.run();
  }

  private void testFailureExceptions(String namespace, List<String> input, List<String> expected)
      throws Exception {
    Result<PCollection<PubsubMessage>, PubsubMessage> result = getPipelineResult(pipeline, input,
        namespace);

    PAssert.that(result.output()).empty();
    pipeline.run();

    PCollection<String> exceptions = result.failures().apply(MapElements
        .into(TypeDescriptors.strings()).via(message -> message.getAttribute("exception_class")));
    PAssert.that(exceptions).containsInAnyOrder(expected);

    pipeline.run();
  }

  @Test
  public void testOutputOnlyUuidMetric() throws Exception {
    String namespace = "rally-study-foo";
    testSuccess(namespace,
        readTestFiles(Arrays.asList(String.format("jwe/%s.ciphertext.json", namespace))),
        readTestFiles(Arrays.asList(String.format("jwe/%s.plaintext.json", namespace))));
  }

  @Test
  public void testOutputUuidMetricAndCounter() throws Exception {
    String namespace = "rally-study-bar";
    testSuccess(namespace,
        readTestFiles(Arrays.asList(String.format("jwe/%s.ciphertext.json", namespace))),
        readTestFiles(Arrays.asList(String.format("jwe/%s.plaintext.json", namespace))));
  }

  @Test
  public void testOutputPioneerNamespace() throws Exception {
    String namespace = "rally-study-foo";
    testSuccess("pioneer-rally",
        readTestFiles(Arrays.asList(String.format("jwe/%s.ciphertext.json", namespace))),
        readTestFiles(Arrays.asList(String.format("jwe/%s.plaintext.json", namespace))));
  }

  @Test
  public void testOutputSchemaJweMappingMissing() throws Exception {
    List<String> expected = Arrays.asList(
        "com.mozilla.telemetry.decoder.rally.DecryptRallyPayloads$DecryptRallyPayloadsException");
    testFailureExceptions("rally-missing",
        readTestFiles(Arrays.asList("jwe/rally-study-foo.ciphertext.json")), expected);
  }

  @Test
  public void testOutputSchemaJweMappingMissingParent() throws Exception {
    List<String> expected = Arrays.asList(
        "com.mozilla.telemetry.decoder.rally.DecryptRallyPayloads$DecryptRallyPayloadsException");
    testFailureExceptions("rally-missing-parent",
        readTestFiles(Arrays.asList("jwe/rally-study-foo.ciphertext.json")), expected);
  }

  @Test
  public void testOutputSchemaJweMappingWrongMapping() throws Exception {
    List<String> expected = Arrays.asList("org.everit.json.schema.ValidationException");
    testFailureExceptions("rally-wrong",
        readTestFiles(Arrays.asList("jwe/rally-study-foo.ciphertext.json")), expected);
  }

  @Test
  public void testOutputKeyNotFound() throws Exception {
    List<String> expected = Arrays
        .asList("com.mozilla.telemetry.ingestion.core.schema.SchemaNotFoundException");
    testFailureExceptions("this-is-not-a-legitimate-namespace",
        readTestFiles(Arrays.asList("jwe/rally-study-foo.ciphertext.json")), expected);
  }

  @Test
  public void testOutputInvalidEncryptionKey() throws Exception {
    List<String> expected = Arrays.asList("org.jose4j.lang.JoseException");
    testFailureExceptions("rally-study-bar",
        readTestFiles(Arrays.asList("jwe/rally-study-foo.ciphertext.json")), expected);
  }

  @Test
  public void testOutputInjectMetadataWrongNamespace() throws Exception {
    List<String> expected = Arrays.asList(
        "com.mozilla.telemetry.decoder.rally.DecryptRallyPayloads$DecryptRallyPayloadsException");
    testFailureExceptions("telemetry-rally",
        readTestFiles(Arrays.asList("jwe/rally-study-foo.ciphertext.json")), expected);
  }

  @Test
  public void testOutputMissingRallyId() throws Exception {
    List<String> input = readTestFiles(Arrays.asList("jwe/rally-study-foo.plaintext.json")).stream()
        .map(x -> {
          try {
            ObjectNode node = Json.readObjectNode(x);
            node.remove("metrics");
            ObjectNode envelope = Json.createObjectNode();
            envelope.put("payload",
                encryptWithTestPublicKey("jwe/rally-study-foo.public.json", node.toString()));
            return envelope.toString();
          } catch (Exception e) {
            return null;
          }
        }).collect(Collectors.toList());

    List<String> expected = Arrays.asList(
        "com.mozilla.telemetry.decoder.rally.DecryptRallyPayloads$DecryptRallyPayloadsException");
    testFailureExceptions("rally-study-foo", input, expected);
  }

  @Test
  public void testInvalidGleanDocument() throws Exception {
    List<String> input = readTestFiles(Arrays.asList("jwe/rally-study-foo.plaintext.json")).stream()
        .map(x -> {
          try {
            ObjectNode node = Json.readObjectNode(x);
            // necessary for glean validation
            node.remove("ping_info");
            ObjectNode envelope = Json.createObjectNode();
            envelope.put("payload",
                encryptWithTestPublicKey("jwe/rally-study-foo.public.json", node.toString()));
            return envelope.toString();
          } catch (Exception e) {
            return null;
          }
        }).collect(Collectors.toList());
    List<String> expected = Arrays.asList("org.everit.json.schema.ValidationException");
    testFailureExceptions("rally-study-foo", input, expected);
  }

  @Test
  public void testEmptyMetadataLocation() throws Exception {
    // minimal test for throughput of a single document
    ValueProvider<String> metadataLocation = pipeline.newProvider(null);
    ValueProvider<String> schemasLocation = pipeline.newProvider("schemas.tar.gz");
    ValueProvider<Boolean> kmsEnabled = pipeline.newProvider(false);
    ValueProvider<Boolean> decompressPayload = pipeline.newProvider(true);

    pipeline.apply(Create.of(readTestFiles(Arrays.asList("jwe/rally-study-foo.ciphertext.json"))))
        .apply(InputFileFormat.text.decode()).apply(DecryptRallyPayloads.of(metadataLocation,
            schemasLocation, kmsEnabled, decompressPayload));

    assertEquals(assertThrows(IllegalArgumentException.class, () -> {
      try {
        pipeline.run();
      } catch (Exception e) {
        throw Throwables.getRootCause(e);
      }
    }).getMessage(), "Metadata location is missing.");
  }
}
