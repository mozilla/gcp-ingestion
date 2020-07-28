package com.mozilla.telemetry.ingestion.sink;

import static org.junit.Assert.assertEquals;

import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.core.transform.PubsubMessageToObjectNode;
import com.mozilla.telemetry.ingestion.core.util.Json;
import com.mozilla.telemetry.ingestion.sink.util.PubsubTopics;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.contrib.java.lang.system.TextFromStandardInputStream;

public class SinkPubsubIntegrationTest {

  @Rule
  public final PubsubTopics pubsub = new PubsubTopics(2);

  @Rule
  public final TextFromStandardInputStream systemIn = TextFromStandardInputStream
      .emptyStandardInputStream();

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  private final PubsubMessageToObjectNode encoder = PubsubMessageToObjectNode.Beam.of();

  @Before
  public void unregisterStackdriver() {
    // unregister stackdriver stats exporter in case a previous test already registered one.
    StackdriverStatsExporter.unregister();
  }

  @Test
  public void canRepublishMessages() throws IOException {
    final List<String> input = new LinkedList<>();
    final List<Map<String, Object>> expected = new LinkedList<>();
    for (int index = 0; index < pubsub.numTopics; index++) {
      String[] topicParts = pubsub.getTopic(index).split("/", 4);
      String project = topicParts[1];
      String topic = topicParts[3];
      PubsubMessage message = PubsubMessage.newBuilder().putAttributes("project", project)
          .putAttributes("topic", topic).build();
      input.add(Json.asString(
          encoder.apply(null, message.getAttributesMap(), message.getData().toByteArray())));
      Map<String, Object> attributes = new HashMap<>(message.getAttributesMap());
      attributes.put("index", index);
      expected.add(attributes);
    }

    systemIn.provideLines(input.toArray(new String[] {}));
    environmentVariables.set("INPUT_PIPE", "-");
    environmentVariables.set("OUTPUT_TOPIC", "projects/${project}/topics/${topic}");

    Sink.main(null);

    List<Map<String, Object>> actual = IntStream.range(0, pubsub.numTopics).boxed()
        .flatMap(index -> pubsub.pull(index, 1, false).stream().map(message -> {
          Map<String, Object> attributes = new HashMap<>(message.getAttributesMap());
          attributes.put("index", index);
          return attributes;
        })).collect(Collectors.toList());

    actual.sort(Comparator.comparing(m -> (Integer) m.get("index")));
    assertEquals(expected, actual);
  }
}
