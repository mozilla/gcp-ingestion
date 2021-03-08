package com.mozilla.telemetry.contextualservices;

import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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

public class FilterByDocTypeTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testMessagesFiltered() {
    List<String> allowedDocTypes = Arrays.asList("type-a", "type-b");

    List<PubsubMessage> inputDocTypes = Stream.of("type-a", "type-b", "type-c", "type-a", "type-d")
        .map(docType -> ImmutableMap.<String, String>of(Attribute.DOCUMENT_TYPE, docType))
        .map(attributes -> new PubsubMessage("{}".getBytes(StandardCharsets.UTF_8), attributes))
        .collect(Collectors.toList());

    PCollection<PubsubMessage> output = pipeline.apply(Create.of(inputDocTypes))
        .apply(FilterByDocType.of(pipeline.newProvider(allowedDocTypes)));

    PAssert.that(output).satisfies(messages -> {
      HashMap<String, Integer> docTypeCount = new HashMap<>();

      for (PubsubMessage message : messages) {
        String docType = message.getAttribute(Attribute.DOCUMENT_TYPE);
        int count = docTypeCount.getOrDefault(docType, 0);
        docTypeCount.put(docType, count + 1);
      }

      Map<String, Integer> expected = ImmutableMap.of("type-a", 2, "type-b", 1);

      Assert.assertEquals(expected, docTypeCount);

      return null;
    });

    pipeline.run();
  }
}
