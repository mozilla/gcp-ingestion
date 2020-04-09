package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class PioneerDecryptor extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  public static PioneerDecryptor of() {
    return new PioneerDecryptor();
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
            return FailureMessage.of(PioneerDecryptor.class.getSimpleName(), //
                ee.element(), //
                ee.exception());
          }
        }));
  }

  private class Fn implements ProcessFunction<PubsubMessage, Iterable<PubsubMessage>> {

    @Override
    public Iterable<PubsubMessage> apply(PubsubMessage message) throws IOException {
      message = PubsubConstraints.ensureNonNull(message);
      Map<String, String> attributes = new HashMap<>(message.getAttributeMap());
      byte[] payload = message.getPayload();
      return Collections.singletonList(new PubsubMessage(payload, attributes));
    }
  }
}
