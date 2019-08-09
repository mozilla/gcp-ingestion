/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.heka;

import static org.junit.Assert.assertEquals;

import com.google.common.io.Resources;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.json.JSONObject;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

public class HekaTest {

  private List<JSONObject> readHekaBlob(String fileName) throws IOException, FileNotFoundException {
    String path = Resources.getResource(fileName).getPath();
    FileInputStream fis = new FileInputStream(path);
    return HekaReader.readHekaStream(fis);
  }

  private JSONObject getJsonBlobFromFile(String fileName)
      throws IOException, FileNotFoundException {
    Path path = Paths.get(Resources.getResource(fileName).getPath());
    return new JSONObject(new String(Files.readAllBytes(path)));
  }

  @Test
  public void testRepeatedEmbeddedBlobParsing() throws IOException, FileNotFoundException {
    // this test validates that we can extract multiple json structures
    // (10 in this case) stored inside a heka blob
    List<JSONObject> res = readHekaBlob("testdata/heka/test_snappy.heka");
    JSONObject expectedObject = getJsonBlobFromFile("testdata/heka/test_snappy.heka.json");
    assertEquals(10, res.size());

    // the only difference between the blobs should be an incrementing
    // sequence key
    int seq = 0;
    for (JSONObject extractedObj : res) {
      expectedObject = expectedObject.put("seq", seq);
      JSONAssert.assertEquals(extractedObj, expectedObject, false);
      seq++;
    }
  }

  @Test
  public void testTelemetryParsing() throws IOException, FileNotFoundException {
    // this test validates that we can parse a reasonably complex heka-encoded structure, very
    // close to what we have in telemetry

    // read the heka-encoded telemetry ping
    String inputHekaFileName = Resources.getResource("testdata/heka/test_telemetry_snappy.heka")
        .getPath();
    FileInputStream fis = new FileInputStream(inputHekaFileName);
    List<JSONObject> res = HekaReader.readHekaStream(fis);

    // just one message in this one
    assertEquals(res.size(), 1);

    // compare against what we expect
    Path expectedJsonPayloadFilePath = Paths
        .get(Resources.getResource("testdata/heka/test_telemetry_snappy.heka.json").getPath());
    JSONObject expectedJsonPing = new JSONObject(
        new String(Files.readAllBytes(expectedJsonPayloadFilePath)));
    JSONAssert.assertEquals(res.get(0), expectedJsonPing, false);
  }
}
