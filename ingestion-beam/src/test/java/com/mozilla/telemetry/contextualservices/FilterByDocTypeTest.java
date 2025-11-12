package com.mozilla.telemetry.contextualservices;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
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

public class FilterByDocTypeTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testMessagesFiltered() {
    FilterByDocType.clearSingletonsForTests();
    String allowedDocTypes = "type-a,type-b,";
    String allowedNamespaces = "ns-1,ns-2,";

    List<PubsubMessage> inputDocTypes = Stream
        .of(List.of("type-a", "ns-1"), List.of("type-b", "ns-2"), List.of("type-b", "ns-3"),
            List.of("type-a", "ns-1"), List.of("type-d", "ns-1"))
        .map(tuple -> ImmutableMap.of(Attribute.DOCUMENT_TYPE, tuple.get(0),
            Attribute.DOCUMENT_NAMESPACE, tuple.get(1)))
        .map(attributes -> new PubsubMessage("{}".getBytes(StandardCharsets.UTF_8), attributes))
        .collect(Collectors.toList());

    PCollection<PubsubMessage> output = pipeline.apply(Create.of(inputDocTypes))
        .apply(FilterByDocType.of(allowedDocTypes, allowedNamespaces, false));

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

  @Test
  public void testRejectFirefoxVersion() {
    FilterByDocType.clearSingletonsForTests();
    // Build list of messages with different doctype/version combinations
    final List<PubsubMessage> input = Streams
        .zip(
            Stream.of("topsites-click", "quick-suggest", "topsites-click", "top-sites", "top-sites",
                "top-sites", "search-with", "search-with", "unsupported-ping"),
            Stream.of("87", "87", "86", "115", "136", "137", "121", "122", "123"),
            (doctype, version) -> ImmutableMap.of(Attribute.DOCUMENT_TYPE, doctype, //
                Attribute.DOCUMENT_NAMESPACE, "contextual-services", //
                Attribute.USER_AGENT_BROWSER, "Firefox", //
                Attribute.USER_AGENT_VERSION, version, //
                Attribute.CLIENT_COMPRESSION, "gzip"))
        .map(attributes -> new PubsubMessage(new byte[] {}, attributes))
        .collect(Collectors.toList());

    PCollection<PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(FilterByDocType.of(
            "topsites-click,quick-suggest,top-sites,search-with,unsupported-ping",
            "contextual-services", true));

    PAssert.that(result).satisfies(messages -> {
      Assert.assertEquals(3, Iterables.size(messages));
      messages.forEach(message -> {
        String documentType = message.getAttribute(Attribute.DOCUMENT_TYPE);
        if ("top-sites".equals(documentType)) {
          Assert.assertEquals("136", message.getAttribute(Attribute.USER_AGENT_VERSION));
        } else if ("topsites-click".equals(documentType)) {
          Assert.assertEquals("87", message.getAttribute(Attribute.USER_AGENT_VERSION));
        } else if ("search-with".equals(documentType)) {
          Assert.assertEquals("122", message.getAttribute(Attribute.USER_AGENT_VERSION));
        } else {
          throw new IllegalArgumentException("unknown document type");
        }
      });
      return null;
    });
    pipeline.run();
  }
}
