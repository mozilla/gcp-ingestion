/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.mozilla.telemetry.utils.Json;
import java.io.IOException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.values.TupleTag;

public class PubsubMessageToTableRow extends MapElementsWithErrors<PubsubMessage, TableRow> {
  public static final TupleTag<TableRow> mainTag = new TupleTag<>();

  @Override
  public TupleTag<TableRow> getMainTag() {
    return mainTag;
  }

  protected TableRow processElement(PubsubMessage element) throws IOException {
    return Json.readTableRow(element.getPayload());
  }
}
