package com.mozilla.telemetry.heka;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.WithErrors.Result;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

@SuppressWarnings("AbbreviationAsWordInName")
public class HekaIOTest extends TestWithDeterministicJson {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  private static ObjectNode getJsonBlobFromFile(String fileName) throws IOException {
    Path path = Paths.get(Resources.getResource(fileName).getPath());
    return Json.readObjectNode(Files.readAllBytes(path));
  }

  @Test
  public void testRepeatedEmbeddedBlobParsing() throws IOException {
    // this test validates that we can extract multiple json structures
    // (10 in this case) stored inside a heka blob
    Result<PCollection<PubsubMessage>> result = pipeline
        .apply(FileIO.match()
            .filepattern(Resources.getResource("testdata/heka/test_snappy.heka").getPath()))
        .apply(FileIO.readMatches()).apply(HekaIO.readFiles());

    PAssert.thatSingleton(result.output().apply(Count.globally())).isEqualTo(10L);
    PAssert.that(result.output()).satisfies(iter -> {
      try {
        ObjectNode expectedObject = getJsonBlobFromFile("testdata/heka/test_snappy.heka.json");
        long seq = 0;
        for (PubsubMessage extractedObj : iter) {
          expectedObject.put("seq", seq);
          expectedObject.remove("meta");
          assertEquals(sortJSON(Json.asString(expectedObject)),
              sortJSON(Json.asString(Json.readObjectNode(extractedObj.getPayload()))));
          seq++;
        }
        return null;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
    pipeline.run();
  }

  @Test
  public void testTelemetryParsing() throws IOException {
    // this test validates that we can parse a reasonably complex heka-encoded structure, very
    // close to what we have in telemetry

    // read the heka-encoded telemetry ping
    PCollection<PubsubMessage> result = pipeline
        .apply(FileIO.match().filepattern(
            Resources.getResource("testdata/heka/test_telemetry_snappy.heka").getPath()))
        .apply(FileIO.readMatches()).apply(HekaIO.readFiles()).output();

    // just one message in this one
    PAssert.thatSingleton(result.apply(Count.globally())).isEqualTo(1L);
    PAssert.thatSingleton(result).satisfies(actual -> {
      try {
        // compare against what we expect
        Path expectedJsonPayloadFilePath = Paths
            .get(Resources.getResource("testdata/heka/test_telemetry_snappy.heka.json").getPath());
        ObjectNode expectedJsonPing = Json
            .readObjectNode(Files.readAllBytes(expectedJsonPayloadFilePath));
        JsonNode meta = expectedJsonPing.remove("meta");

        assertEquals(sortJSON(Json.asString(expectedJsonPing)),
            sortJSON(Json.asString(Json.readObjectNode(actual.getPayload()))));
        assertEquals(meta.path("documentId").textValue(),
            actual.getAttribute(Attribute.DOCUMENT_ID));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return null;
    });

    pipeline.run();
  }

  @Test
  public void testCrashPingParsing() throws IOException {
    // this test validates that we can parse a reasonably complex heka-encoded structure, very
    // close to what we have in telemetry

    // read the heka-encoded crash ping
    PCollection<PubsubMessage> result = pipeline
        .apply(FileIO.match()
            .filepattern(Resources.getResource("testdata/heka/crashping.heka").getPath()))
        .apply(FileIO.readMatches()).apply(HekaIO.readFiles()).output();

    // just one message in this one
    PAssert.thatSingleton(result.apply(Count.globally())).isEqualTo(1L);
    PAssert.thatSingleton(result).satisfies(actual -> {
      Path expectedJsonPayloadFilePath = Paths
          .get(Resources.getResource("testdata/heka/crashping.heka.json").getPath());
      try {
        ObjectNode expectedJsonPing = Json
            .readObjectNode(Files.readAllBytes(expectedJsonPayloadFilePath));
        JsonNode meta = expectedJsonPing.remove("meta");
        assertEquals(sortJSON(Json.asString(expectedJsonPing)),
            sortJSON(Json.asString(Json.readObjectNode(actual.getPayload()))));

        assertEquals(meta.path("documentId").textValue(),
            actual.getAttribute(Attribute.DOCUMENT_ID));
        assertEquals("2019-08-31T18:23:43.471129088Z",
            actual.getAttribute(Attribute.SUBMISSION_TIMESTAMP));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return null;
    });

    pipeline.run();
  }

}
