/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.heka;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.junit.Test;

public class HekaReaderTest extends TestWithDeterministicJson {

  private List<PubsubMessage> readHekaBlob(String fileName) throws IOException {
    String path = Resources.getResource(fileName).getPath();
    FileInputStream fis = new FileInputStream(path);
    return readHekaStream(fis);
  }

  private ObjectNode getJsonBlobFromFile(String fileName) throws IOException {
    Path path = Paths.get(Resources.getResource(fileName).getPath());
    return Json.readObjectNode(Files.readAllBytes(path));
  }

  @Test
  public void testRepeatedEmbeddedBlobParsing() throws IOException {
    // this test validates that we can extract multiple json structures
    // (10 in this case) stored inside a heka blob
    List<PubsubMessage> res = readHekaBlob("testdata/heka/test_snappy.heka");
    ObjectNode expectedObject = getJsonBlobFromFile("testdata/heka/test_snappy.heka.json");
    assertEquals(10, res.size());

    // the only difference between the blobs should be an incrementing
    // sequence key
    long seq = 0;
    for (PubsubMessage extractedObj : res) {
      expectedObject.put("seq", seq);
      JsonNode meta = expectedObject.remove("meta");
      assertEquals(sortJSON(Json.asString(expectedObject)),
          sortJSON(Json.asString(Json.readObjectNode(extractedObj.getPayload()))));
      seq++;
    }
  }

  @Test
  public void testTelemetryParsing() throws IOException {
    // this test validates that we can parse a reasonably complex heka-encoded structure, very
    // close to what we have in telemetry

    // read the heka-encoded telemetry ping
    String inputHekaFileName = Resources.getResource("testdata/heka/test_telemetry_snappy.heka")
        .getPath();
    FileInputStream fis = new FileInputStream(inputHekaFileName);
    List<PubsubMessage> res = readHekaStream(fis);

    // just one message in this one
    assertEquals(res.size(), 1);
    PubsubMessage actual = res.get(0);

    // compare against what we expect
    Path expectedJsonPayloadFilePath = Paths
        .get(Resources.getResource("testdata/heka/test_telemetry_snappy.heka.json").getPath());
    ObjectNode expectedJsonPing = Json
        .readObjectNode(Files.readAllBytes(expectedJsonPayloadFilePath));
    JsonNode meta = expectedJsonPing.remove("meta");

    assertEquals(sortJSON(Json.asString(expectedJsonPing)),
        sortJSON(Json.asString(Json.readObjectNode(actual.getPayload()))));
    assertEquals(meta.path("documentId").textValue(), actual.getAttribute(Attribute.DOCUMENT_ID));
  }

  @Test
  public void testCrashPingParsing() throws IOException {
    // this test validates that we can parse a reasonably complex heka-encoded structure, very
    // close to what we have in telemetry

    // read the heka-encoded crash ping
    String inputHekaFileName = Resources.getResource("testdata/heka/crashping.heka").getPath();
    FileInputStream fis = new FileInputStream(inputHekaFileName);
    List<PubsubMessage> res = readHekaStream(fis);

    // just one message in this one
    assertEquals(res.size(), 1);
    PubsubMessage actual = res.get(0);

    // compare against what we expect
    Path expectedJsonPayloadFilePath = Paths
        .get(Resources.getResource("testdata/heka/crashping.heka.json").getPath());
    ObjectNode expectedJsonPing = Json
        .readObjectNode(Files.readAllBytes(expectedJsonPayloadFilePath));
    JsonNode meta = expectedJsonPing.remove("meta");
    assertEquals(sortJSON(Json.asString(expectedJsonPing)),
        sortJSON(Json.asString(Json.readObjectNode(actual.getPayload()))));
    assertEquals(meta.path("documentId").textValue(), actual.getAttribute(Attribute.DOCUMENT_ID));
  }

  private static List<PubsubMessage> readHekaStream(InputStream is) throws IOException {
    List<PubsubMessage> decodedMessages = new ArrayList<>();

    while (true) {
      PubsubMessage o = HekaReader.readHekaMessage(is);
      if (o == null) {
        break;
      }
      decodedMessages.add(o);
    }
    return decodedMessages;
  }


}
