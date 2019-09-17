/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.heka;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.MapElementsWithErrors;
import com.mozilla.telemetry.util.Json;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

public class HekaIO {

  public static ReadFiles readFiles() {
    return ReadFiles.INSTANCE;
  }

  public static class ReadFiles extends MapElementsWithErrors<ReadableFile, List<PubsubMessage>> {

    @Override
    protected List<PubsubMessage> processElement(ReadableFile readableFile) throws IOException {
      List<PubsubMessage> pubsubMessages = new ArrayList<PubsubMessage>();
      ByteArrayInputStream bis = new ByteArrayInputStream(readableFile.readFullyAsBytes());
      for (ObjectNode json : HekaReader.readHekaStream(bis)) {
        pubsubMessages.add(new PubsubMessage(Json.asBytes(json), null));
      }
      return pubsubMessages;
    }

    @Override
    protected PubsubMessage processError(ReadableFile readableFile, Exception e) {
      return FailureMessage.of(this, readableFile, e);
    }

    private static ReadFiles INSTANCE = new ReadFiles();

    private ReadFiles() {
    }

  }

}
