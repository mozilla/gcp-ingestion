package com.mozilla.telemetry.ingestion.sink;

import static org.junit.Assert.assertEquals;

import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.BoundedSink;
import com.mozilla.telemetry.ingestion.sink.util.PubsubTopics;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class SinkPubsubIntegrationTest {

  @Rule
  public final PubsubTopics pubsub = new PubsubTopics(3);

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Test
  public void canRepublishMessages() throws IOException {
    final List<Map<String, Object>> expected = IntStream.range(1, pubsub.numTopics)
        .mapToObj(index -> {
          String[] topicParts = pubsub.getTopic(index).split("/", 4);
          String project = topicParts[1];
          String topic = topicParts[3];
          PubsubMessage message = PubsubMessage.newBuilder().putAttributes("project", project)
              .putAttributes("topic", topic).build();
          pubsub.publish(0, message);
          Map<String, Object> attributes = new HashMap<>(message.getAttributesMap());
          attributes.put("index", index);
          return attributes;
        }).collect(Collectors.toList());

    environmentVariables.set("INPUT_SUBSCRIPTION", pubsub.getSubscription(0));
    environmentVariables.set("OUTPUT_TOPIC", "projects/${project}/topics/${topic}");

    BoundedSink.run(expected.size(), 30);

    List<Map<String, Object>> actual = IntStream.range(1, pubsub.numTopics).boxed()
        .flatMap(index -> pubsub.pull(index, 2, true).stream().map(message -> {
          Map<String, Object> attributes = new HashMap<>(message.getAttributesMap());
          attributes.put("index", index);
          return attributes;
        })).collect(Collectors.toList());

    actual.sort(Comparator.comparing(m -> (Integer) m.get("index")));
    assertEquals(expected, actual);
  }
}
