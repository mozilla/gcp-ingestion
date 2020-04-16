package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.KeyStore;
import java.io.IOException;
import java.security.PrivateKey;
import java.util.Collections;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.jose4j.jwe.JsonWebEncryption;
import org.jose4j.lang.JoseException;

public class DecryptPioneerPayloads extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  private final ValueProvider<String> metadataLocation;
  private transient KeyStore keyStore;

  public static DecryptPioneerPayloads of(ValueProvider<String> metadataLocation) {
    return new DecryptPioneerPayloads(metadataLocation);
  }

  private DecryptPioneerPayloads(ValueProvider<String> metadataLocation) {
    this.metadataLocation = metadataLocation;
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

  private class Fn implements ProcessFunction<PubsubMessage, Iterable<PubsubMessage>> {

    @Override
    public Iterable<PubsubMessage> apply(PubsubMessage message) throws IOException, JoseException {
      message = PubsubConstraints.ensureNonNull(message);

      if (keyStore == null) {
        keyStore = KeyStore.of(metadataLocation.get());
      }

      // TODO: count per doctype errors
      ObjectNode json = Json.readObjectNode(message.getPayload());

      PrivateKey key = keyStore.getKey(message.getAttribute(Attribute.DOCUMENT_NAMESPACE));
      JsonWebEncryption jwe = new JsonWebEncryption();
      jwe.setKey(key);
      jwe.setContentEncryptionKey(key.getEncoded());
      jwe.setCompactSerialization(json.get("payload").asText());
      return Collections
          .singletonList(new PubsubMessage(jwe.getPlaintextBytes(), message.getAttributeMap()));
    }
  }
}
