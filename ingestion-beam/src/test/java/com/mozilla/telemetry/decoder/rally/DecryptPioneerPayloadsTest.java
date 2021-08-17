package com.mozilla.telemetry.decoder.rally;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.ingestion.core.Constant.FieldName;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.util.GzipUtil;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithFailures.Result;
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

  private String modifyEncryptionKeyId(String jsonData, String newEncryptionKeyId)
      throws Exception {
    ObjectNode json = Json.readObjectNode(jsonData);
    ((ObjectNode) json.get(FieldName.PAYLOAD)).put(DecryptPioneerPayloads.ENCRYPTION_KEY_ID,
        newEncryptionKeyId);
    return json.toString();
  }

  private String modifySchemaNamespace(String jsonData, String newSchemaNamespace)
      throws Exception {
    ObjectNode json = Json.readObjectNode(jsonData);
    ((ObjectNode) json.get(FieldName.PAYLOAD)).put(DecryptPioneerPayloads.SCHEMA_NAMESPACE,
        newSchemaNamespace);
    return json.toString();
  }

  private String removeRequiredSchemaNamespace(String jsonData) throws Exception {
    ObjectNode json = Json.readObjectNode(jsonData);
    // This effectively turns the schema into a v1 pioneer-study ping
    ((ObjectNode) json.get(FieldName.PAYLOAD)).remove(DecryptPioneerPayloads.SCHEMA_NAMESPACE);
    return json.toString();
  }

  /** Remove metadata inserted by the pioneer pipeline and assert they existed.
   * If the do not exist, return null. */
  public static String removePioneerMetadata(String jsonData) {
    try {
      ObjectNode node = Json.readObjectNode(jsonData);
      assertNotNull(node.remove(DecryptPioneerPayloads.PIONEER_ID));
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
      return input
          .apply(MapElements.into(TypeDescriptors.strings()).via(s -> removePioneerMetadata(s)));
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

    String payload = Json.readObjectNode(ciphertext).get("payload")
        .get(DecryptPioneerPayloads.ENCRYPTED_DATA).textValue();
    byte[] decrypted = DecryptPioneerPayloads.decrypt(key, payload);
    byte[] decompressed = GzipUtil.maybeDecompress(decrypted);

    String actual = Json.readObjectNode(plaintext).toString();
    String expect = Json.readObjectNode(decompressed).toString();
    Assert.assertEquals(expect, actual);
  }

  @Test
  public void testOutput() throws Exception {
    // minimal test for throughput of a single document
    String metadataLocation = Resources.getResource("pioneer/metadata-local.json").getPath();
    Boolean kmsEnabled = false;
    Boolean decompressPayload = true;

    final List<String> input = readTestFiles(Arrays.asList("pioneer/study-foo.ciphertext.json"));
    PCollection<String> output = pipeline.apply(Create.of(input))
        .apply(InputFileFormat.text.decode())
        .apply("AddAttributes", MapElements.into(TypeDescriptor.of(PubsubMessage.class))
            .via(element -> new PubsubMessage(element.getPayload(),
                ImmutableMap.of(Attribute.DOCUMENT_NAMESPACE, "telemetry", Attribute.DOCUMENT_TYPE,
                    "pioneer-study", Attribute.DOCUMENT_VERSION, "4"))))
        .apply(DecryptPioneerPayloads.of(metadataLocation, kmsEnabled, decompressPayload)).output()
        .apply(OutputFileFormat.text.encode()).apply(ReformatJson.of());

    final List<String> expectedMain = readTestFiles(Arrays.asList("pioneer/sample.plaintext.json"));
    PAssert.that(output).containsInAnyOrder(expectedMain);

    pipeline.run();
  }

  @Test
  public void testOutputDeletionAndEnrollment() throws Exception {
    // minimal test for throughput of a single document
    String metadataLocation = Resources.getResource("pioneer/metadata-local.json").getPath();
    Boolean kmsEnabled = false;
    Boolean decompressPayload = true;

    final List<String> input = readTestFiles(Arrays.asList("pioneer/deletion-request.sample.json",
        "pioneer/pioneer-enrollment.sample.json"));
    Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline.apply(Create.of(input))
        .apply(InputFileFormat.text.decode())
        .apply("AddAttributes", MapElements.into(TypeDescriptor.of(PubsubMessage.class))
            .via(element -> new PubsubMessage(element.getPayload(),
                ImmutableMap.of(Attribute.DOCUMENT_NAMESPACE, "telemetry", Attribute.DOCUMENT_TYPE,
                    "pioneer-study", Attribute.DOCUMENT_VERSION, "4"))))
        .apply(DecryptPioneerPayloads.of(metadataLocation, kmsEnabled, decompressPayload));

    PAssert.that(result.failures()).empty();
    // pipeline run must be called for each one of these branches, otherwise it
    // will skip over each of the intermediate collections and just go to last
    // seen collection.
    pipeline.run();

    PCollection<String> output = result.output().apply(OutputFileFormat.text.encode())
        .apply(ReformatJson.of());
    final List<String> expectedMain = Arrays.asList("{}", "{}");
    PAssert.that(output).containsInAnyOrder(expectedMain);
    pipeline.run();
  }

  @Test
  public void testErrors() throws Exception {
    // minimal test for throughput of a single document
    String metadataLocation = Resources.getResource("pioneer/metadata-local.json").getPath();
    Boolean kmsEnabled = false;
    Boolean decompressPayload = true;

    final List<String> input = readTestFiles(Arrays.asList("pioneer/study-foo.ciphertext.json",
        "pioneer/study-foo.ciphertext.json", "pioneer/study-foo.ciphertext.json"));
    input.set(0, modifyEncryptionKeyId(input.get(0), "invalid-key")); // IOException
    input.set(1, modifyEncryptionKeyId(input.get(1), "study-bar")); // JoseException
    input.set(2, removeRequiredSchemaNamespace(input.get(2))); // ValidationException

    Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline.apply(Create.of(input))
        .apply(InputFileFormat.text.decode())
        .apply("AddAttributes", MapElements.into(TypeDescriptor.of(PubsubMessage.class))
            .via(element -> new PubsubMessage(element.getPayload(),
                ImmutableMap.of(Attribute.DOCUMENT_NAMESPACE, "telemetry", Attribute.DOCUMENT_TYPE,
                    "pioneer-study", Attribute.DOCUMENT_VERSION, "4"))))
        .apply(DecryptPioneerPayloads.of(metadataLocation, kmsEnabled, decompressPayload));

    PAssert.that(result.output()).empty();
    pipeline.run();

    PCollection<String> exceptions = result.failures().apply(MapElements
        .into(TypeDescriptors.strings()).via(message -> message.getAttribute("exception_class")));
    // IntegrityException extends JoseException
    PAssert.that(exceptions).containsInAnyOrder("java.io.IOException",
        "org.jose4j.lang.JoseException", "org.everit.json.schema.ValidationException");

    pipeline.run();
  }

  @Test
  public void testEmptyMetadataLocation() throws Exception {
    // minimal test for throughput of a single document
    String metadataLocation = null;
    Boolean kmsEnabled = false;
    Boolean decompressPayload = true;

    pipeline.apply(Create.of(readTestFiles(Arrays.asList("pioneer/study-foo.ciphertext.json"))))
        .apply(InputFileFormat.text.decode())
        .apply(DecryptPioneerPayloads.of(metadataLocation, kmsEnabled, decompressPayload));

    assertEquals(assertThrows(IllegalArgumentException.class, () -> {
      try {
        pipeline.run();
      } catch (Exception e) {
        throw Throwables.getRootCause(e);
      }
    }).getMessage(), "Metadata location is missing.");
  }

  @Test
  public void testBug1674847() throws Exception {
    // minimal test for invalid enrollment pings sending addonId instead of
    // schemaNamespace
    String metadataLocation = Resources.getResource("pioneer/metadata-local.json").getPath();
    Boolean kmsEnabled = false;
    Boolean decompressPayload = true;

    final List<String> input = readTestFiles(
        Arrays.asList("pioneer/study-foo.ciphertext.json", "pioneer/study-foo.ciphertext.json"));
    input.set(0, modifySchemaNamespace(input.get(0), "news.study@princeton.edu"));
    input.set(1, modifySchemaNamespace(input.get(1), "pioneer-citp-news-disinfo"));

    Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline.apply(Create.of(input))
        .apply(InputFileFormat.text.decode())
        .apply("AddAttributes", MapElements.into(TypeDescriptor.of(PubsubMessage.class))
            .via(element -> new PubsubMessage(element.getPayload(),
                ImmutableMap.of(Attribute.DOCUMENT_NAMESPACE, "telemetry", Attribute.DOCUMENT_TYPE,
                    "pioneer-study", Attribute.DOCUMENT_VERSION, "4"))))
        .apply(DecryptPioneerPayloads.of(metadataLocation, kmsEnabled, decompressPayload));

    PAssert.that(result.failures()).empty();
    pipeline.run();

    PCollection<String> output = result.output().apply(MapElements.into(TypeDescriptors.strings())
        .via(message -> message.getAttribute(Attribute.DOCUMENT_NAMESPACE)));
    final List<String> expectedNamespaces = Arrays.asList("pioneer-citp-news-disinfo",
        "pioneer-citp-news-disinfo");
    PAssert.that(output).containsInAnyOrder(expectedNamespaces);
    pipeline.run();
  }

  @Test
  public void testBug1691807EnrollmentDeletionPayloadsSuccess() throws Exception {
    // test valid payloads within the enrollment and deletion request
    String metadataLocation = Resources.getResource("pioneer/metadata-local.json").getPath();
    Boolean kmsEnabled = false;
    Boolean decompressPayload = true;

    final List<String> input = readTestFiles(
        Arrays.asList("pioneer/bug-1691807.pioneer-enrollment.valid.study-bar.ciphertext.json",
            "pioneer/bug-1691807.deletion-request.valid.study-bar.ciphertext.json"));

    Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline.apply(Create.of(input))
        .apply(InputFileFormat.text.decode())
        .apply("AddAttributes", MapElements.into(TypeDescriptor.of(PubsubMessage.class))
            .via(element -> new PubsubMessage(element.getPayload(),
                ImmutableMap.of(Attribute.DOCUMENT_NAMESPACE, "telemetry", Attribute.DOCUMENT_TYPE,
                    "pioneer-study", Attribute.DOCUMENT_VERSION, "4"))))
        .apply(DecryptPioneerPayloads.of(metadataLocation, kmsEnabled, decompressPayload));

    PAssert.that(result.failures()).empty();
    pipeline.run();

    PCollection<String> output = result.output().apply(OutputFileFormat.text.encode())
        .apply(ReformatJson.of());
    final List<String> expectedMain = readTestFiles(
        Arrays.asList("pioneer/sample.plaintext.json", "pioneer/sample.plaintext.json"));
    PAssert.that(output).containsInAnyOrder(expectedMain);
    pipeline.run();
  }

  @Test
  public void testBug1691807EnrollmentDeletionPayloadsFailures() throws Exception {
    // test valid payloads within the enrollment and deletion request
    String metadataLocation = Resources.getResource("pioneer/metadata-local.json").getPath();
    Boolean kmsEnabled = false;
    Boolean decompressPayload = true;

    final List<String> input = readTestFiles(
        Arrays.asList("pioneer/bug-1691807.pioneer-enrollment.invalid.study-bar.ciphertext.json",
            "pioneer/bug-1691807.deletion-request.invalid.study-bar.ciphertext.json"));

    Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline.apply(Create.of(input))
        .apply(InputFileFormat.text.decode())
        .apply("AddAttributes", MapElements.into(TypeDescriptor.of(PubsubMessage.class))
            .via(element -> new PubsubMessage(element.getPayload(),
                ImmutableMap.of(Attribute.DOCUMENT_NAMESPACE, "telemetry", Attribute.DOCUMENT_TYPE,
                    "pioneer-study", Attribute.DOCUMENT_VERSION, "4"))))
        .apply(DecryptPioneerPayloads.of(metadataLocation, kmsEnabled, decompressPayload));

    PAssert.that(result.failures()).empty();
    pipeline.run();

    PCollection<String> output = result.output().apply(OutputFileFormat.text.encode())
        .apply(ReformatJson.of());
    final List<String> expectedMain = Arrays.asList("{}", "{}");
    PAssert.that(output).containsInAnyOrder(expectedMain);
    pipeline.run();
  }

  @Test
  public void testBug1703648() throws Exception {
    // minimal test for invalid enrollment pings sending addonId instead of
    // schemaNamespace
    String metadataLocation = Resources.getResource("pioneer/metadata-local.json").getPath();
    Boolean kmsEnabled = false;
    Boolean decompressPayload = true;

    final List<String> input = readTestFiles(Arrays.asList("pioneer/study-foo.ciphertext.json"));
    input.set(0, modifySchemaNamespace(input.get(0), "rally-foo"));

    Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline.apply(Create.of(input))
        .apply(InputFileFormat.text.decode())
        .apply("AddAttributes", MapElements.into(TypeDescriptor.of(PubsubMessage.class))
            .via(element -> new PubsubMessage(element.getPayload(),
                ImmutableMap.of(Attribute.DOCUMENT_NAMESPACE, "telemetry", Attribute.DOCUMENT_TYPE,
                    "pioneer-study", Attribute.DOCUMENT_VERSION, "4"))))
        .apply(DecryptPioneerPayloads.of(metadataLocation, kmsEnabled, decompressPayload));

    PAssert.that(result.failures()).empty();
    pipeline.run();

    PCollection<String> output = result.output()
        .apply(MapElements.into(TypeDescriptors.strings()).via(message -> {
          try {
            return Json.readObjectNode(message.getPayload()).get("rallyId").asText();
          } catch (Exception e) {
            return null;
          }
        }));
    final List<String> expectedId = Arrays.asList("9af045aa-80ca-e743-92de-b31431abc547");
    PAssert.that(output).containsInAnyOrder(expectedId);
    pipeline.run();
  }
}
