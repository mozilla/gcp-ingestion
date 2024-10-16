package com.mozilla.telemetry.contextualservices;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class VerifyMetadataTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testRejectUncompressed() {
    Map<String, String> baseAttributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "topsites-click",
        Attribute.DOCUMENT_NAMESPACE, "contextual-services", //
        Attribute.USER_AGENT_BROWSER, "Firefox", //
        Attribute.USER_AGENT_VERSION, "90");

    Map<String, String> gzipCompressed = ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.CLIENT_COMPRESSION, "gzip").build();
    Map<String, String> otherCompressed = ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.CLIENT_COMPRESSION, "other").build();

    final List<PubsubMessage> input = Arrays.asList(
        new PubsubMessage(new byte[] {}, baseAttributes),
        new PubsubMessage(new byte[] {}, gzipCompressed),
        new PubsubMessage(new byte[] {}, otherCompressed));

    WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(VerifyMetadata.of());

    PAssert.that(result.failures()).satisfies(messages -> {
      Assert.assertEquals(2, Iterables.size(messages));
      PubsubMessage message = Iterables.get(messages, 0);

      String errorMessage = message.getAttribute("error_message");
      Assert.assertTrue(errorMessage.contains(RejectedMessageException.class.getCanonicalName()));
      Assert.assertTrue(errorMessage.contains("gzip"));

      return null;
    });

    PAssert.that(result.output()).satisfies(messages -> {
      Assert.assertEquals(1, Iterables.size(messages));
      Assert.assertEquals("gzip",
          Iterables.get(messages, 0).getAttribute(Attribute.CLIENT_COMPRESSION));
      return null;
    });

    pipeline.run();
  }

  @Test
  public void testRejectUserAgent() {
    Map<String, String> attributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "topsites-click", //
        Attribute.DOCUMENT_NAMESPACE, "contextual-services", //
        Attribute.USER_AGENT_BROWSER, "Firefoxd", //
        Attribute.USER_AGENT_VERSION, "90", //
        Attribute.CLIENT_COMPRESSION, "gzip");

    final List<PubsubMessage> input = Collections
        .singletonList(new PubsubMessage(new byte[] {}, attributes));

    WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(VerifyMetadata.of());

    PAssert.that(result.failures()).satisfies(messages -> {
      Assert.assertEquals(1, Iterables.size(messages));
      PubsubMessage message = Iterables.get(messages, 0);

      String errorMessage = message.getAttribute("error_message");
      Assert.assertTrue(errorMessage.contains(RejectedMessageException.class.getCanonicalName()));
      Assert.assertTrue(errorMessage.contains("user agent"));

      return null;
    });

    pipeline.run();
  }

  @Test
  public void testRejectIspCountry() {
    Map<String, String> baseAttributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "topsites-click",
        Attribute.DOCUMENT_NAMESPACE, "contextual-services", //
        Attribute.CLIENT_COMPRESSION, "gzip", //
        Attribute.USER_AGENT_BROWSER, "Firefox", //
        Attribute.USER_AGENT_VERSION, "90");

    Map<String, String> shouldThrow = ImmutableMap.<String, String>builder().putAll(baseAttributes)
        .put(Attribute.GEO_COUNTRY, "IN").put(Attribute.ISP_NAME, "Infonet Comm Enterprises")
        .build();
    Map<String, String> validIsp = ImmutableMap.<String, String>builder().putAll(baseAttributes)
        .put(Attribute.GEO_COUNTRY, "IN").put(Attribute.ISP_NAME, "Other ISP").build();
    Map<String, String> validCountry = ImmutableMap.<String, String>builder().putAll(baseAttributes)
        .put(Attribute.GEO_COUNTRY, "US").put(Attribute.ISP_NAME, "Infonet Comm Enterprises")
        .build();

    final List<PubsubMessage> input = Arrays.asList(
        new PubsubMessage(new byte[] {}, baseAttributes),
        new PubsubMessage(new byte[] {}, shouldThrow), new PubsubMessage(new byte[] {}, validIsp),
        new PubsubMessage(new byte[] {}, validCountry));

    WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(VerifyMetadata.of());

    PAssert.that(result.failures()).satisfies(messages -> {
      Assert.assertEquals(1, Iterables.size(messages));
      PubsubMessage message = Iterables.get(messages, 0);

      String errorMessage = message.getAttribute("error_message");
      Assert.assertTrue(errorMessage.contains(RejectedMessageException.class.getCanonicalName()));
      Assert.assertTrue(errorMessage.contains("CONSVC-1764"));

      return null;
    });

    PAssert.that(result.output()).satisfies(messages -> {
      Assert.assertEquals(3, Iterables.size(messages));
      return null;
    });

    pipeline.run();
  }

  @Test
  public void testPassNonDesktop() {
    // Build list of messages with different doctype/version combinations
    final List<PubsubMessage> input = Streams
        .zip(Stream.of("topsites-impression", "topsites-click"),
            Stream.of("org-mozilla-firefox-beta", "contextual-services"),
            (doctype, namespace) -> ImmutableMap.of(Attribute.DOCUMENT_TYPE, doctype, //
                Attribute.CLIENT_COMPRESSION, "gzip", //
                Attribute.DOCUMENT_NAMESPACE, namespace))
        .map(attributes -> new PubsubMessage(new byte[] {}, attributes))
        .collect(Collectors.toList());

    WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(VerifyMetadata.of());

    PAssert.that(result.failures()).satisfies(messages -> {
      Assert.assertEquals(1, Iterables.size(messages));
      PubsubMessage message = Iterables.get(messages, 0);

      String errorMessage = message.getAttribute("error_message");
      Assert.assertTrue(errorMessage.contains(RejectedMessageException.class.getCanonicalName()));

      return null;
    });

    PAssert.that(result.output()).satisfies(messages -> {
      Assert.assertEquals(1, Iterables.size(messages));
      Assert.assertEquals("topsites-impression",
          Iterables.get(messages, 0).getAttribute(Attribute.DOCUMENT_TYPE));
      Assert.assertEquals("org-mozilla-firefox-beta",
          Iterables.get(messages, 0).getAttribute(Attribute.DOCUMENT_NAMESPACE));

      return null;
    });

    pipeline.run();
  }

}
