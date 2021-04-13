package com.mozilla.telemetry.util;

import com.fasterxml.jackson.core.JsonParser;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

/**
 * {@link CustomCoder} for {@link PubsubMessage} via JSON that allows null payload and message
 * values for testing.
 */
public class PubsubWithNullsJsonCoder extends CustomCoder<PubsubMessage> {

  private PubsubWithNullsJsonCoder() {
  }

  private static PubsubWithNullsJsonCoder INSTANCE = new PubsubWithNullsJsonCoder();

  public static PubsubWithNullsJsonCoder of() {
    return INSTANCE;
  }

  static class Json extends com.mozilla.telemetry.util.Json {

    static {
      MAPPER.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    }

    /**
     * Read a {@link PubsubMessage} from a string and allow null results.
     */
    public static PubsubMessage readPubsubMessage(InputStream data) throws IOException {
      return MAPPER.readValue(data, PubsubMessage.class);
    }
  }

  @Override
  public void encode(PubsubMessage value, OutputStream outStream)
      throws CoderException, IOException {
    outStream.write(Json.asBytes(value));
  }

  @Override
  public PubsubMessage decode(InputStream inStream) throws CoderException, IOException {
    return Json.readPubsubMessage(inStream);
  }
}
