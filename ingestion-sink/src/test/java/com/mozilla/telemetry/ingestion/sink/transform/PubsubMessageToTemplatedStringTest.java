package com.mozilla.telemetry.ingestion.sink.transform;

import static org.junit.Assert.assertEquals;

import com.google.pubsub.v1.PubsubMessage;
import org.junit.Test;

public class PubsubMessageToTemplatedStringTest {

  private static final PubsubMessage EMPTY_MESSAGE = PubsubMessage.newBuilder().build();

  @Test
  public void canResolveStaticTemplateWithEmptyMessage() {
    assertEquals("static", PubsubMessageToTemplatedString.of("static").apply(EMPTY_MESSAGE));
  }

  @Test
  public void canResolveDerivedAttributes() {
    assertEquals("2019-01-01/23",
        PubsubMessageToTemplatedString.of("${submission_date}/${submission_hour}")
            .apply(PubsubMessage.newBuilder()
                .putAttributes("submission_timestamp", "2019-01-01T23:00:00").build()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void failsOnMissingAttributes() {
    PubsubMessageToTemplatedString.of("${missing}").apply(EMPTY_MESSAGE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void failsOnMalformedTemplate() {
    PubsubMessageToTemplatedString.of("${").apply(EMPTY_MESSAGE);
  }
}
