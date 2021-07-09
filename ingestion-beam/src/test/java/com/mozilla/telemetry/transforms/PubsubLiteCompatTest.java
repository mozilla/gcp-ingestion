package com.mozilla.telemetry.transforms;

import static org.junit.Assert.assertEquals;

import com.google.cloud.pubsublite.proto.AttributeValues;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import com.mozilla.telemetry.ingestion.core.util.IOFunction;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.util.Json;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

public class PubsubLiteCompatTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  private final List<PubsubMessage> outputToLite = ImmutableList.of(//
      new PubsubMessage(new byte[] {}, null), //
      new PubsubMessage("payload".getBytes(StandardCharsets.UTF_8),
          ImmutableMap.of("meta", "data")));

  private static final Function<com.google.cloud.pubsublite.proto.PubSubMessage, //
      String> jsonPrinter = IOFunction.unchecked(JsonFormat.printer()::print);

  private static String liteToJson(com.google.cloud.pubsublite.proto.PubSubMessage message) {
    return jsonPrinter.apply(message);
  }

  private static final List<String> expectedToLite = Stream.of(//
      com.google.cloud.pubsublite.proto.PubSubMessage.newBuilder()
          .setData(ByteString.copyFromUtf8("")).build(),
      com.google.cloud.pubsublite.proto.PubSubMessage.newBuilder()
          .setData(ByteString.copyFromUtf8("payload"))
          .putAttributes("meta",
              AttributeValues.newBuilder().addValues(ByteString.copyFromUtf8("data")).build())
          .build())
      .map(PubsubLiteCompatTest::liteToJson).collect(Collectors.toList());

  @Test
  public void testToPubsubLiteFunction() throws Exception {
    List<String> actual = outputToLite.stream().map(PubsubLiteCompat::toPubsubLite)
        .map(PubsubLiteCompatTest::liteToJson).collect(Collectors.toList());
    assertEquals(expectedToLite, actual);
  }

  @Test
  public void testToPubsubLiteTransform() throws Exception {
    PCollection<String> result = pipeline //
        .apply(Create.of(outputToLite)) //
        .apply(PubsubLiteCompat.toPubsubLite()) //
        .apply("liteToJson",
            MapElements.into(TypeDescriptors.strings()).via(PubsubLiteCompatTest::liteToJson));
    PAssert.that(result).containsInAnyOrder(expectedToLite);
    pipeline.run();
  }

  private static final List<SequencedMessage> inputFromLite = ImmutableList.of(
      SequencedMessage.newBuilder()
          .setMessage(com.google.cloud.pubsublite.proto.PubSubMessage.newBuilder().build()).build(),
      SequencedMessage.newBuilder()
          .setMessage(com.google.cloud.pubsublite.proto.PubSubMessage.newBuilder()
              .setData(ByteString.copyFromUtf8("payload"))
              .putAttributes("meta",
                  AttributeValues.newBuilder().addValues(ByteString.copyFromUtf8("data")).build())
              .build())
          .setCursor(Cursor.newBuilder().setOffset(1).build()).build());

  private static final List<String> expectedFromLite = Stream.of(//
      new PubsubMessage("".getBytes(StandardCharsets.UTF_8), ImmutableMap.of()),
      new PubsubMessage("payload".getBytes(StandardCharsets.UTF_8),
          ImmutableMap.of("meta", "data", "message_id", "1")))
      .map(IOFunction.unchecked(Json::asString)).collect(Collectors.toList());

  @Test
  public void testFromPubsubLiteFunction() throws Exception {
    List<String> actual = inputFromLite.stream().map(PubsubLiteCompat::fromPubsubLite)
        .map(IOFunction.unchecked(Json::asString)).collect(Collectors.toList());
    assertEquals(expectedFromLite, actual);
  }

  @Test
  public void testFromPubsubLiteTransform() throws Exception {
    PCollection<String> result = pipeline //
        .apply(Create.of(inputFromLite)) //
        .apply(PubsubLiteCompat.fromPubsubLite()) //
        .apply("OutputFileFormat.json", OutputFileFormat.json.encode());
    PAssert.that(result).containsInAnyOrder(expectedFromLite);
    pipeline.run();
  }
}
