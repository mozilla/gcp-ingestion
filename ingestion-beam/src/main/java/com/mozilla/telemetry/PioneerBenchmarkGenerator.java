package com.mozilla.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.PublicKey;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.jose4j.jwk.EcJwkGenerator;
import org.jose4j.jwk.EllipticCurveJsonWebKey;
import org.jose4j.keys.EllipticCurves;
import org.jose4j.lang.JoseException;

public class PioneerBenchmarkGenerator {

  final static ObjectMapper mapper = new ObjectMapper();

  public static byte[] encrypt(byte[] data, PublicKey key) {
    return data;
  }

  public static Optional<String> transform(String data, PublicKey key) {
    try {
      PubsubMessage message = Json.readPubsubMessage(data);
      PubsubMessage encryptedMessage = new PubsubMessage(encrypt(message.getPayload(), key),
          message.getAttributeMap());
      return Optional.of(Json.asString(encryptedMessage));
    } catch (IOException e) {
      e.printStackTrace();
      return Optional.empty();
    }
  }

  public static void main(final String[] args) throws JoseException {
    EllipticCurveJsonWebKey key = EcJwkGenerator.generateJwk(EllipticCurves.P256);
    // write the key to disk
    // write the metadata file

    try (Stream<String> stream = Files.lines(Paths.get("document_sample.ndjson"))) {

      Files.write(Paths.get("output.ndjson"),
          (Iterable<String>) stream.map(s -> transform(s, key.getPublicKey()))
              .filter(Optional::isPresent).map(Optional::get)::iterator);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}