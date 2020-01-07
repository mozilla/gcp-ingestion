package com.mozilla.telemetry.ingestion.sink.util;

import com.google.pubsub.v1.PubsubMessage;
import java.util.List;

/**
 * No-parameter overloads for {@link PubsubTopics} when {@code numTopics = 1}.
 */
public class SinglePubsubTopic extends PubsubTopics {

  public SinglePubsubTopic() {
    super(1);
  }

  public String getSubscription() {
    return getSubscription(0);
  }

  public String getTopic() {
    return getTopic(0);
  }

  public String publish(PubsubMessage message) {
    return publish(0, message);
  }

  public List<PubsubMessage> pull(int maxMessages, boolean returnImmediately) {
    return pull(0, maxMessages, returnImmediately);
  }
}
