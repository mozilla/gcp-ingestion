/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.heka;

import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.MapElementsWithErrors;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.json.JSONObject;

public class HekaTransform extends MapElementsWithErrors<ReadableFile, List<PubsubMessage>> {

  @Override
  protected List<PubsubMessage> processElement(ReadableFile readableFile) throws IOException {
    List<PubsubMessage> pubsubMessages = new ArrayList<PubsubMessage>();
    ByteArrayInputStream bis = new ByteArrayInputStream(readableFile.readFullyAsBytes());
    for (JSONObject json : HekaReader.readHekaStream(bis)) {
      pubsubMessages.add(new PubsubMessage(json.toString().getBytes(), null));
    }
    return pubsubMessages;
  }

  @Override
  protected PubsubMessage processError(ReadableFile readableFile, Exception e) {
    return FailureMessage.of(this, readableFile, e);
  }

}
