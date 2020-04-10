package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.BeamFileInputStream;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.PrivateKey;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.internal.IoUtils;
import org.jose4j.jwe.JsonWebEncryption;
import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.lang.JoseException;

public class DecryptPioneerPayloads extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  private final ValueProvider<String> keysLocation;
  private transient KeyStore keyStore;

  public static DecryptPioneerPayloads of(ValueProvider keysLocation) {
    return new DecryptPioneerPayloads(keysLocation);
  }

  private DecryptPioneerPayloads(ValueProvider<String> keysLocation) {
    this.keysLocation = keysLocation;
  }

  @Override
  public Result<PCollection<PubsubMessage>, PubsubMessage> expand(
      PCollection<PubsubMessage> messages) {
    return messages.apply(FlatMapElements.into(TypeDescriptor.of(PubsubMessage.class)) //
        .via(new Fn()) //
        .exceptionsInto(TypeDescriptor.of(PubsubMessage.class)) //
        .exceptionsVia((WithFailures.ExceptionElement<PubsubMessage> ee) -> {
          try {
            throw ee.exception();
          } catch (IOException e) {
            return FailureMessage.of(DecryptPioneerPayloads.class.getSimpleName(), //
                ee.element(), //
                ee.exception());
          }
        }));
  }

  private class KeyStore {

    public PrivateKey getKey(String path) {
      ensureKeysLoaded();
      PrivateKey key = keys.get(path);
      return key;
    }

    private final String keysLocation;
    private transient Map<String, PrivateKey> keys;

    // enable this to read from gcs via BeamFileInputStream
    public KeyStore(String keysLocation) {
      this.keysLocation = keysLocation;
    }

    private void loadAllKeys() throws IOException {
      final Map<String, PrivateKey> tempKeys = new HashMap<>();
      InputStream inputStream;
      try {
        inputStream = BeamFileInputStream.open(this.keysLocation);
      } catch (IOException e) {
        throw new IOException("Exception thrown while reading private key");
      }
      byte[] serializedKeyBytes = IoUtils.toByteArray(inputStream);
      String serializedKey = new String(serializedKeyBytes);

      try {
        PublicJsonWebKey key = PublicJsonWebKey.Factory.newPublicJwk(serializedKey);
        tempKeys.put("*", key.getPrivateKey());
      } catch (JoseException e) {
        throw new RuntimeException(e);
      }

      keys = tempKeys;
    }

    private void ensureKeysLoaded() {
      if (keys == null) {
        try {
          loadAllKeys();
        } catch (IOException e) {
          throw new UncheckedIOException("unexpected error while loading keys", e);
        }
      }
    }
  }

  private class Fn implements ProcessFunction<PubsubMessage, Iterable<PubsubMessage>> {

    @Override
    public Iterable<PubsubMessage> apply(PubsubMessage message) throws IOException, JoseException {
      message = PubsubConstraints.ensureNonNull(message);

      if (keyStore == null) {
        keyStore = new KeyStore(keysLocation.get());
      }

      // TODO: count per doctype errors
      ObjectNode json = Json.readObjectNode(message.getPayload());

      PrivateKey key = keyStore.getKey("*");
      JsonWebEncryption jwe = new JsonWebEncryption();
      jwe.setKey(key);
      jwe.setContentEncryptionKey(key.getEncoded());
      jwe.setCompactSerialization(json.get("payload").asText());
      return Collections
          .singletonList(new PubsubMessage(jwe.getPlaintextBytes(), message.getAttributeMap()));
    }
  }
}
