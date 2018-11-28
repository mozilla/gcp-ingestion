/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

public class PubsubMessageToTableRow extends MapElementsWithErrors<PubsubMessage, TableRow> {

  protected TableRow processElement(PubsubMessage element) throws IOException {
    return Json.readTableRow(element.getPayload());
  }
}
