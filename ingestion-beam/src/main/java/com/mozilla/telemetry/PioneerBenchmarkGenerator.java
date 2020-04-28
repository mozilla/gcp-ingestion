package com.mozilla.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.PublicKey;
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

  public static Optional<String> transform(String data, PublicKey key) {
    try {
      PubsubMessage message = Json.readPubsubMessage(data);
      PubsubMessage encryptedMessage = new PubsubMessage(encrypt(message.getPayload(), key),
          message.getAttributeMap());
      return Optional.of(Json.asString(encryptedMessage));
    } catch (IOException | JoseException e) {
      e.printStackTrace();
      return Optional.empty();
    }
  }

  public static void main(final String[] args) throws JoseException {
    EllipticCurveJsonWebKey key = EcJwkGenerator.generateJwk(EllipticCurves.P256);
    // write the key to disk
    // write the metadata file

    try (Stream<String> stream = Files.lines(Paths.get("document_sample.ndjson"))) {

      Files.write(Paths.get("pioneer_benchmark.ndjson"),
          (Iterable<String>) stream.map(s -> transform(s, key.getPublicKey()))
              .filter(Optional::isPresent).map(Optional::get)::iterator);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}