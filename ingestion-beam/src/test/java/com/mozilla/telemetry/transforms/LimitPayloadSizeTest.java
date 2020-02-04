package com.mozilla.telemetry.transforms;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.mozilla.telemetry.options.InputFileFormat;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;

public class LimitPayloadSizeTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testLimit() {
    List<String> passingPayloads = ImmutableList.of("", "abcdefg",
        StringUtils.repeat("abcdefg", 50));
    List<String> failingPayloads = ImmutableList.of(StringUtils.repeat("abcdefghij", 51));

    WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline //
        .apply(Create.of(Iterables.concat(passingPayloads, failingPayloads))) //
        .apply(InputFileFormat.text.decode()) //
        .apply("LimitPayloadSize", LimitPayloadSize.toBytes(500));

    PAssert
        .that(result.output().apply("get success payload",
            MapElements.into(TypeDescriptors.strings()).via(m -> new String(m.getPayload())))) //
        .containsInAnyOrder(passingPayloads);
    PAssert
        .that(result.failures().apply("get failure payload",
            MapElements.into(TypeDescriptors.strings()).via(m -> new String(m.getPayload())))) //
        .containsInAnyOrder(failingPayloads);

    pipeline.run();
  }
}
