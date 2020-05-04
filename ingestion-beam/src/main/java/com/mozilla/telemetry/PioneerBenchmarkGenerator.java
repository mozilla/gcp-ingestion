package com.mozilla.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.jose4j.jwe.ContentEncryptionAlgorithmIdentifiers;
import org.jose4j.jwe.JsonWebEncryption;
import org.jose4j.jwe.KeyManagementAlgorithmIdentifiers;
import org.jose4j.jwk.EcJwkGenerator;
import org.jose4j.jwk.EllipticCurveJsonWebKey;
import org.jose4j.jwk.JsonWebKey.OutputControlLevel;
import org.jose4j.keys.EllipticCurves;
import org.jose4j.lang.JoseException;

/** Generate a dataset for benchmarking the DecryptPioneerPayloads transform. */
public class PioneerBenchmarkGenerator {

  static final ObjectMapper mapper = new ObjectMapper();

  /** Encrypt a payload using a public key and insert it into an envelope. */
  public static byte[] encrypt(byte[] data, PublicKey key) throws IOException, JoseException {
    JsonWebEncryption jwe = new JsonWebEncryption();
    jwe.setPayload(new String(data, Charsets.UTF_8));
    jwe.setAlgorithmHeaderValue(KeyManagementAlgorithmIdentifiers.ECDH_ES);
    jwe.setEncryptionMethodHeaderParameter(ContentEncryptionAlgorithmIdentifiers.AES_256_GCM);
    jwe.setKey(key);
    String serializedJwe = jwe.getCompactSerialization();
    ObjectNode node = mapper.createObjectNode();
    node.put("payload", serializedJwe);
    return Json.asString(node).getBytes(Charsets.UTF_8);
  }

  /** Read a Pubsub ndjson message wrapping failures in an Optional. */
  public static Optional<PubsubMessage> parsePubsub(String data) {
    try {
      return Optional.of(Json.readPubsubMessage(data));
    } catch (Exception e) {
      e.printStackTrace();
      return Optional.empty();
    }
  }

  /** Encrypt the payload in a Pubsub message and place it into an envelope. */
  public static Optional<String> transform(PubsubMessage message, PublicKey key,
      byte[] exampleData) {
    try {
      HashMap<String, String> attributes = new HashMap<String, String>(message.getAttributeMap());
      ObjectNode node = Json.readObjectNode(exampleData);
      ObjectNode payload = (ObjectNode) node.get("payload");

      payload.put("encryptedData", new String(encrypt(message.getPayload(), key)));
      payload.put("encryptionKeyId", attributes.get(Attribute.DOCUMENT_NAMESPACE));
      payload.put("schemaNamespace", attributes.get(Attribute.DOCUMENT_NAMESPACE));
      payload.put("schemaName", attributes.get(Attribute.DOCUMENT_TYPE));
      payload.put("schemaVersion", attributes.get(Attribute.DOCUMENT_VERSION));
      attributes.put(Attribute.DOCUMENT_NAMESPACE, "telemetry");
      attributes.put(Attribute.DOCUMENT_TYPE, "pioneer-study");
      attributes.put(Attribute.DOCUMENT_VERSION, "4");

      PubsubMessage encryptedMessage = new PubsubMessage(
          Json.asString(node).getBytes(Charsets.UTF_8), attributes);
      return Optional.of(Json.asString(encryptedMessage));
    } catch (IOException | JoseException e) {
      e.printStackTrace();
      return Optional.empty();
    }
  }

  /** Read in documents in the shape of Pubsub ndjson files and encrypt using
   * Pioneer parameters and envelope. Write out the relevant metadata files for
   * the single key in use. */
  public static void main(final String[] args) throws JoseException, IOException {
    final Path inputPath = Paths.get("document_sample.ndjson");
    final Path outputPath = Paths.get("pioneer_benchmark_data.ndjson");
    final Path keyPath = Paths.get("pioneer_benchmark_key.json");
    final Path metadataPath = Paths.get("pioneer_benchmark_metadata.json");
    final Path examplePath = Paths
        .get("src/test/resources/pioneer/telemetry.pioneer-study.4.sample.pass.json");
    final byte[] exampleData = Files.readAllBytes(examplePath);

    EllipticCurveJsonWebKey key = EcJwkGenerator.generateJwk(EllipticCurves.P256);
    HashSet<String> namespaces = new HashSet<String>();

    // runs in ~2:30 min
    try (Stream<String> stream = Files.lines(inputPath)) {
      // side-effects to get the set of attributes
      Files.write(outputPath, (Iterable<String>) stream.map(PioneerBenchmarkGenerator::parsePubsub)
          .filter(Optional::isPresent).map(Optional::get).map(message -> {
            // side-effects and side-input
            namespaces.add(message.getAttribute("document_namespace"));
            return transform(message, key.getPublicKey(), exampleData);
          }).filter(Optional::isPresent).map(Optional::get)::iterator);
    } catch (IOException e) {
      e.printStackTrace();
    }

    // write out the key
    Files.write(keyPath, key.toJson(OutputControlLevel.INCLUDE_PRIVATE).getBytes(Charsets.UTF_8));

    // write out the metadata
    ArrayNode metadata = mapper.createArrayNode();
    for (String namespace : namespaces) {
      ObjectNode node = mapper.createObjectNode();
      node.put("private_key_id", namespace);
      // TODO: write this to the appropriate location like gcs, if relevant
      node.put("private_key_uri", keyPath.toString());
      // empty value
      node.put("kms_resource_id", "");
      metadata.add(node);
    }
    Files.write(metadataPath, metadata.toPrettyString().getBytes(Charsets.UTF_8));

  }
}
