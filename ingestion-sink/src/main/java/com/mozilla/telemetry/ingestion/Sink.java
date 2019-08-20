/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion;

import com.google.api.gax.batching.FlowControlSettings;
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
    BigQuery.Write output = new BigQuery.Write(BigQueryOptions.getDefaultInstance().getService(),
        // PubsubMessageToTableRow reports protobuf size, which can be ~1/3rd more efficient than
        // the JSON that actually gets sent over HTTP, so we use 60% of the API limit by default.
        Env.getInt("BATCH_MAX_BYTES", 6_000_000), // HTTP request size limit: 10 MB
        Env.getInt("BATCH_MAX_MESSAGES", 10_000), // Maximum rows per request: 10,000
        Env.getLong("BATCH_MAX_DELAY_MILLIS", 1000L)); // Default 1 second

    // read pubsub messages from INPUT_SUBSCRIPTION
    new Pubsub.Read(Env.getString("INPUT_SUBSCRIPTION"),
        message -> CompletableFuture.supplyAsync(() -> message) // start new future with message
            .thenApplyAsync(routeMessage::apply) // determine output path of message
            .thenComposeAsync(output), // output message
        builder -> builder.setFlowControlSettings(FlowControlSettings.newBuilder()
            .setMaxOutstandingElementCount(
                // Upstream default is 10K, but it can be higher as long as we don't OOM
                Env.getLong("FLOW_CONTROL_MAX_OUTSTANDING_ELEMENT_COUNT", 50_000L)) // 50K
            .setMaxOutstandingRequestBytes(
                // Upstream default is 1GB, but it needs to be lower so we don't OOM
                Env.getLong("FLOW_CONTROL_MAX_OUTSTANDING_REQUEST_BYTES", 100_000_000L)) // 100MB
            .build())).run(); // run pubsub consumer
  }
}
