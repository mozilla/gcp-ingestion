package com.mozilla.telemetry.contextualservices;

import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class FilterByChannelTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testMessagesFiltered() {
    String allowedChannels = "release,beta";

    List<PubsubMessage> inputChannels = Stream
        .of("release", "beta", "nightly", "release", "badInput")
        .map(channel -> ImmutableMap.of(Attribute.NORMALIZED_CHANNEL, channel))
        .map(attributes -> new PubsubMessage("{}".getBytes(StandardCharsets.UTF_8), attributes))
        .collect(Collectors.toList());

    PCollection<PubsubMessage> output = pipeline.apply(Create.of(inputChannels))
        .apply(FilterByChannel.of(allowedChannels));

    PAssert.that(output).satisfies(messages -> {
      HashMap<String, Integer> channelCount = new HashMap<>();

      for (PubsubMessage message : messages) {
        String channel = message.getAttribute(Attribute.NORMALIZED_CHANNEL);
        int count = channelCount.getOrDefault(channel, 0);
        channelCount.put(channel, count + 1);
      }

      Map<String, Integer> expected = ImmutableMap.of("release", 2, "beta", 1);

      Assert.assertEquals(expected, channelCount);

      return null;
    });

    pipeline.run();
  }
}
