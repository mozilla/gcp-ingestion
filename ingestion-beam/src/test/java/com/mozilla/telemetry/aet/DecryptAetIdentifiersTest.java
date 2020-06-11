package com.mozilla.telemetry.aet;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.ingestion.core.schema.JSONSchemaStore;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.JsonValidator;
import com.mozilla.telemetry.util.KeyStore;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.jose4j.jwe.ContentEncryptionAlgorithmIdentifiers;
import org.jose4j.jwe.JsonWebEncryption;
import org.jose4j.jwe.KeyManagementAlgorithmIdentifiers;
import org.jose4j.jwk.PublicJsonWebKey;
import org.junit.Rule;
import org.junit.Test;

public class DecryptAetIdentifiersTest extends TestWithDeterministicJson {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  /** Load a public key from a JWK. See the KeyStore for more details. */
  private static PublicJsonWebKey loadPublicKey(String resourceLocation) throws Exception {
    byte[] data = Resources.toByteArray(Resources.getResource(resourceLocation));
    return PublicJsonWebKey.Factory.newPublicJwk(new String(data));
  }

  private static String encryptWithTestPublicKey(String payload) throws Exception {
    PublicJsonWebKey key = loadPublicKey("account-ecosystem/testkey1.public.json");
    JsonWebEncryption jwe = new JsonWebEncryption();
    jwe.setKey(key.getKey());
    jwe.setKeyIdHeaderValue(key.getKeyId());
    jwe.setAlgorithmHeaderValue(KeyManagementAlgorithmIdentifiers.ECDH_ES_A256KW);
    jwe.setEncryptionMethodHeaderParameter(ContentEncryptionAlgorithmIdentifiers.AES_256_GCM);
    jwe.setPayload(payload);
    return jwe.getCompactSerialization();
  }

  @Test
  public void testDecrypt() throws Exception {
    String expected = "abc123";
    KeyStore keyStore = KeyStore.of("src/test/resources/account-ecosystem/metadata-local.json",
        false);
    String encrypted = encryptWithTestPublicKey(expected);
    String actual = DecryptAetIdentifiers.decrypt(keyStore, TextNode.valueOf(encrypted))
        .textValue();
    assertEquals(expected, actual);
  }

  @Test
  public void testSanitizeJsonNode() throws Exception {
    String userId = "2ef67665ec7369ba0fab7f802a5e1e04";
    String anonId = encryptWithTestPublicKey(userId);
    ObjectNode json = Json.asObjectNode(
        ImmutableMap.of("payload", ImmutableMap.of("myList", ImmutableList.of("foo", anonId))));
    ObjectNode expected = Json.asObjectNode(ImmutableMap.of("payload",
        ImmutableMap.of("myList", ImmutableList.of("foo", "eyJr<435 characters redacted>"))));
    DecryptAetIdentifiers.sanitizeJsonNode(json);
    assertEquals(Json.asString(expected), Json.asString(json));
  }

  @Test(expected = ValidationException.class)
  public void testForbiddenFieldsStructured() throws Exception {
    JsonValidator validator = new JsonValidator();
    byte[] data = Resources
        .toByteArray(Resources.getResource("account-ecosystem/structured.schema.json"));
    Schema fxaSchema = JSONSchemaStore.readSchema(data);
    ObjectNode json = Json.asObjectNode(ImmutableMap.<String, Object>builder()
        .put("ecosystem_client_id", "3ed15efab7e94757bf9e9ef5e844ada2")
        .put("ecosystem_device_id", "7ab4e373ce434b848a9d0946e388fee9")
        .put("ecosystem_user_id", "aab4e373ce434b848a9d0946e388fdd3").build());
    validator.validate(fxaSchema, json);
  }

  @Test
  public void testOutputStructured() throws Exception {
    // minimal test for throughput of a single document
    ValueProvider<String> metadataLocation = pipeline
        .newProvider(Resources.getResource("account-ecosystem/metadata-local.json").getPath());
    ValueProvider<Boolean> kmsEnabled = pipeline.newProvider(false);

    String userId = "2ef67665ec7369ba0fab7f802a5e1e04";
    String anonId = encryptWithTestPublicKey(userId);
    List<String> prevUserIds = ImmutableList.of("3ef67665ec7369ba0fab7f802a5e1e04",
        "4ef67665ec7369ba0fab7f802a5e1e04");
    List<String> prevAnonIds = prevUserIds.stream().map(x -> {
      try {
        return encryptWithTestPublicKey(x);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
    String input = Json.asString(ImmutableMap.builder().put("ecosystem_anon_id", anonId)
        .put("ecosystem_client_id", "3ed15efab7e94757bf9e9ef5e844ada2")
        .put("ecosystem_device_id", "7ab4e373ce434b848a9d0946e388fee9")
        .put("previous_ecosystem_anon_ids", prevAnonIds).build());
    String expected = Json.asString(ImmutableMap.builder().put("ecosystem_user_id", userId)
        .put("ecosystem_client_id", "3ed15efab7e94757bf9e9ef5e844ada2")
        .put("ecosystem_device_id", "7ab4e373ce434b848a9d0946e388fee9")
        .put("previous_ecosystem_user_ids", prevUserIds).build());
    Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline.apply(Create.of(input))
        .apply(InputFileFormat.text.decode())
        .apply("AddAttributes",
            MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                .via(element -> new PubsubMessage(element.getPayload(),
                    ImmutableMap.of(Attribute.URI,
                        "/submit/firefox-accounts/account-ecosystem/1/"
                            + "8299c050-6be8-4197-8a0b-fd1062a11da4"))))
        .apply(DecryptAetIdentifiers.of(metadataLocation, kmsEnabled));
    PAssert.that(result.failures()).empty();
    PAssert.that(result.output().apply(OutputFileFormat.text.encode()))
        .containsInAnyOrder(ImmutableList.of(expected));
    pipeline.run();
  }

  @Test
  public void testOutputTelemetry() throws Exception {
    // minimal test for throughput of a single document
    ValueProvider<String> metadataLocation = pipeline
        .newProvider(Resources.getResource("account-ecosystem/metadata-local.json").getPath());
    ValueProvider<Boolean> kmsEnabled = pipeline.newProvider(false);

    String userId = "2ef67665ec7369ba0fab7f802a5e1e04";
    String anonId = encryptWithTestPublicKey(userId);
    List<String> prevUserIds = ImmutableList.of("3ef67665ec7369ba0fab7f802a5e1e04",
        "4ef67665ec7369ba0fab7f802a5e1e04");
    List<String> prevAnonIds = prevUserIds.stream().map(x -> {
      try {
        return encryptWithTestPublicKey(x);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
    String input = Json.asString(ImmutableMap.of("payload",
        ImmutableMap.builder().put("ecosystemAnonId", anonId)
            .put("ecosystemClientId", "3ed15efab7e94757bf9e9ef5e844ada2")
            .put("ecosystemDeviceId", "7ab4e373ce434b848a9d0946e388fee9")
            .put("previousEcosystemAnonIds", prevAnonIds).build()));
    String expected = Json.asString(ImmutableMap.of("payload",
        ImmutableMap.builder().put("ecosystemUserId", userId)
            .put("ecosystemClientId", "3ed15efab7e94757bf9e9ef5e844ada2")
            .put("ecosystemDeviceId", "7ab4e373ce434b848a9d0946e388fee9")
            .put("previousEcosystemUserIds", prevUserIds).build()));
    Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline.apply(Create.of(input))
        .apply(InputFileFormat.text.decode())
        .apply("AddAttributes",
            MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                .via(element -> new PubsubMessage(element.getPayload(),
                    ImmutableMap.of(Attribute.URI,
                        "/submit/telemetry/ce39b608-f595-4c69-b6a6-f7a436604648"
                            + "/account-ecosystem/Firefox/61.0a1/nightly/20180328030202"))))
        .apply(DecryptAetIdentifiers.of(metadataLocation, kmsEnabled));
    PAssert.that(result.failures()).empty();
    PAssert.that(result.output().apply(OutputFileFormat.text.encode()))
        .containsInAnyOrder(ImmutableList.of(expected));
    pipeline.run();
  }

  @Test
  public void testErrorOutput() throws Exception {
    // minimal test for throughput of a single document
    ValueProvider<String> metadataLocation = pipeline
        .newProvider(Resources.getResource("account-ecosystem/metadata-local.json").getPath());
    ValueProvider<Boolean> kmsEnabled = pipeline.newProvider(false);

    String userId = "2ef67665ec7369ba0fab7f802a5e1e04";
    String anonId = encryptWithTestPublicKey(userId);
    List<String> prevUserIds = ImmutableList.of("3ef67665ec7369ba0fab7f802a5e1e04",
        "4ef67665ec7369ba0fab7f802a5e1e04");
    List<String> prevAnonIds = prevUserIds.stream().map(x -> {
      try {
        return encryptWithTestPublicKey(x);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
    String input = Json.asString(ImmutableMap.builder()
        // We incorrectly name the anonId field like telemetry to cause a failure.
        .put("ecosystemAnonId", anonId)
        .put("ecosystem_client_id", "3ed15efab7e94757bf9e9ef5e844ada2")
        .put("ecosystem_device_id", "7ab4e373ce434b848a9d0946e388fee9")
        .put("previous_ecosystem_anon_ids", prevAnonIds).build());
    String expected = "{\"ecosystemAnonId\":\"eyJr<435 characters redacted>\""
        + ",\"ecosystem_client_id\":\"3ed15efab7e94757bf9e9ef5e844ada2\""
        + ",\"ecosystem_device_id\":\"7ab4e373ce434b848a9d0946e388fee9\""
        + ",\"previous_ecosystem_anon_ids\":[\"eyJr<435 characters redacted>\""
        + ",\"eyJr<435 characters redacted>\"]}";
    Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline.apply(Create.of(input))
        .apply(InputFileFormat.text.decode())
        .apply("AddAttributes",
            MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                .via(element -> new PubsubMessage(element.getPayload(),
                    ImmutableMap.of(Attribute.URI,
                        "/submit/firefox-accounts/account-ecosystem/1/"
                            + "8299c050-6be8-4197-8a0b-fd1062a11da4"))))
        .apply(DecryptAetIdentifiers.of(metadataLocation, kmsEnabled));
    PAssert.that(result.output()).empty();
    PAssert.that(result.failures().apply(OutputFileFormat.text.encode()))
        .containsInAnyOrder(ImmutableList.of(expected));
    pipeline.run();
  }

  @Test
  public void testTelemetryErrorOutput() throws Exception {
    // minimal test for throughput of a single document
    ValueProvider<String> metadataLocation = pipeline
        .newProvider(Resources.getResource("account-ecosystem/metadata-local.json").getPath());
    ValueProvider<Boolean> kmsEnabled = pipeline.newProvider(false);

    List<String> input = ImmutableList.of("{}", "{\"payload\":{}}",
        "{\"payload\":{\"ecosystemClientId\":\"foo\",\"ecosystemDeviceId\":\"bar\"}}");
    List<String> expectedOutput = ImmutableList
        .of("{\"payload\":{\"ecosystemClientId\":\"foo\",\"ecosystemDeviceId\":\"bar\"}}");
    List<String> expectedError = ImmutableList.of("{}", "{\"payload\":{}}");
    Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline.apply(Create.of(input))
        .apply(InputFileFormat.text.decode())
        .apply("AddAttributes",
            MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                .via(element -> new PubsubMessage(element.getPayload(),
                    ImmutableMap.of(Attribute.URI,
                        "/submit/telemetry/ce39b608-f595-4c69-b6a6-f7a436604648"
                            + "/account-ecosystem/Firefox/61.0a1/nightly/20180328030202"))))
        .apply(DecryptAetIdentifiers.of(metadataLocation, kmsEnabled));
    PAssert.that(result.output().apply("EncodeOutput", OutputFileFormat.text.encode()))
        .containsInAnyOrder(expectedOutput);
    PAssert.that(result.failures().apply("EncodeErrors", OutputFileFormat.text.encode()))
        .containsInAnyOrder(expectedError);
    pipeline.run();
  }

}
