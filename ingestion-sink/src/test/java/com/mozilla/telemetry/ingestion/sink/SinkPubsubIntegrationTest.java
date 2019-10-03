package com.mozilla.telemetry.ingestion.sink;

import static org.junit.Assert.assertEquals;

import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.io.Pubsub;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToObjectNode;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToObjectNode.Format;
import com.mozilla.telemetry.ingestion.sink.util.TestWithPubsubResources;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class SinkPubsubIntegrationTest extends TestWithPubsubResources {

  private static final PubsubMessageToObjectNode TO_JSON_OBJECT = new PubsubMessageToObjectNode(
      Format.raw);

  protected int numTopics() {
    return 3;
  }

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Test
  public void canRepublishMessages() {
    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    List<Map<String, Object>> expected = IntStream.range(1, numTopics()).mapToObj(index -> {
      String[] topicParts = getTopic(index).split("/", 4);
      String project = topicParts[1];
      String topic = topicParts[3];
      PubsubMessage message = PubsubMessage.newBuilder().putAttributes("project", project)
          .putAttributes("topic", topic).build();
      publish(0, message);
      Map<String, Object> attributes = new HashMap<>(message.getAttributesMap());
      attributes.put("index", index);
      return attributes;
    }).collect(Collectors.toList());

    environmentVariables.set("INPUT_SUBSCRIPTION", getSubscription(0));
    environmentVariables.set("OUTPUT_TOPIC", "projects/${project}/topics/${topic}");

    Pubsub.Read pubsubRead = Sink.main();
    CompletableFuture<Void> main = CompletableFuture.runAsync(pubsubRead::run);

    List<Map<String, Object>> actual = IntStream.range(1, numTopics()).boxed().flatMap(index -> {
      List<PubsubMessage> delivered;
      do {
        delivered = pull(index, 2, true);
      } while (delivered.size() == 0);
      return delivered.stream().map(message -> {
        Map<String, Object> attributes = new HashMap<>(message.getAttributesMap());
        attributes.put("index", index);
        return attributes;
      });
    }).collect(Collectors.toList());

    pubsubRead.subscriber.stopAsync();
    main.join();

    actual.sort(Comparator.comparing(m -> (Integer) m.get("index")));
    assertEquals(expected, actual);
  }
}
