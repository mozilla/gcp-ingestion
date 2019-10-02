package com.mozilla.telemetry.ingestion.sink.util;

import com.google.pubsub.v1.PubsubMessage;
import java.util.List;

public abstract class TestWithSinglePubsubTopic extends TestWithPubsubResources {

  protected int numTopics() {
    return 1;
  }

  protected String getSubscription() {
    return getSubscription(0);
  }

  protected String getTopic() {
    return getTopic(0);
  }

  protected String publish(PubsubMessage message) {
    return publish(0, message);
  }

  protected List<PubsubMessage> pull(int maxMessages, boolean returnImmediately) {
    return pull(0, maxMessages, returnImmediately);
  }
}
