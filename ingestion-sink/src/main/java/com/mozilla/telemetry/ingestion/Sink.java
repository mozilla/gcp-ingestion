/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion;

import com.google.cloud.bigquery.BigQueryOptions;
import com.mozilla.telemetry.ingestion.io.BigQuery;
import com.mozilla.telemetry.ingestion.io.Pubsub;
import com.mozilla.telemetry.ingestion.transform.PubsubMessageToTableRow;
import com.mozilla.telemetry.ingestion.util.Env;
import java.util.concurrent.CompletableFuture;

public class Sink {

  private Sink() {
  }

  public static void main(String[] args) {
    // route messages to OUTPUT_TABLE with OUTPUT_FORMAT
    PubsubMessageToTableRow routeMessage = new PubsubMessageToTableRow(
        Env.getString("OUTPUT_TABLE"),
        PubsubMessageToTableRow.TableRowFormat.valueOf(Env.getString("OUTPUT_FORMAT", "raw")));

    // output messages to BigQuery
    BigQuery.Write output = new BigQuery.Write(BigQueryOptions.getDefaultInstance().getService());

    // read pubsub messages from INPUT_SUBSCRIPTION
    new Pubsub.Read(Env.getString("INPUT_SUBSCRIPTION"),
        message -> CompletableFuture.supplyAsync(() -> message) // start new future with message
            .thenApplyAsync(routeMessage::apply) // determine output path of message
            .thenComposeAsync(output)) // output message
                .run(); // run pubsub consumer
  }
}
