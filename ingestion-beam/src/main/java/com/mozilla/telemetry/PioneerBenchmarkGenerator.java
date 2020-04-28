package com.mozilla.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PublicKey;
import java.util.HashSet;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.jose4j.jwe.ContentEncryptionAlgorithmIdentifiers;
import org.jose4j.jwe.JsonWebEncryption;
import org.jose4j.jwe.KeyManagementAlgorithmIdentifiers;
import org.jose4j.jwk.EcJwkGenerator;
import org.jose4j.jwk.EllipticCurveJsonWebKey;
import org.jose4j.keys.EllipticCurves;
import org.jose4j.lang.JoseException;

public class PioneerBenchmarkGenerator {

  final static ObjectMapper mapper = new ObjectMapper();

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

  public static Optional<PubsubMessage> parsePubsub(String data) {
    try {
      return Optional.of(Json.readPubsubMessage(data));
    } catch (Exception e) {
      e.printStackTrace();
      return Optional.empty();
    }
  }

  public static Optional<String> transform(PubsubMessage message, PublicKey key) {
    try {
      PubsubMessage encryptedMessage = new PubsubMessage(encrypt(message.getPayload(), key),
          message.getAttributeMap());
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
    Path inputPath = Paths.get("document_sample.ndjson");
    Path outputPath = Paths.get("pioneer_benchmark_data.ndjson");
    Path keyPath = Paths.get("pioneer_benchmark_key.ndjson");
    Path metadataPath = Paths.get("pioneer_benchmark_metadata.json");

    EllipticCurveJsonWebKey key = EcJwkGenerator.generateJwk(EllipticCurves.P256);
    HashSet<String> namespaces = new HashSet<String>();

    // runs in ~2:30 min
    try (Stream<String> stream = Files.lines(inputPath)) {
      // side-effects to get the set of attributes
      Files.write(outputPath, (Iterable<String>) stream.map(PioneerBenchmarkGenerator::parsePubsub)
          .filter(Optional::isPresent).map(Optional::get).map(message -> {
            // side-effects and side-input
            namespaces.add(message.getAttribute("document_namespace"));
            return transform(message, key.getPublicKey());
          }).filter(Optional::isPresent).map(Optional::get)::iterator);
    } catch (IOException e) {
      e.printStackTrace();
    }

    // write out the key
    Files.write(keyPath, key.toJson().getBytes(Charsets.UTF_8));

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
