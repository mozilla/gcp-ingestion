/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.sink;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.bigquery.BigQueryOptions;
import com.mozilla.telemetry.ingestion.sink.io.BigQuery;
import com.mozilla.telemetry.ingestion.sink.io.Pubsub;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToMap.Format;
import com.mozilla.telemetry.ingestion.sink.util.Env;

public class Sink {

  private Sink() {
  }

  public static void main(String[] args) {
    // output messages to BigQuery
    BigQuery.Write output = new BigQuery.Write(BigQueryOptions.getDefaultInstance().getService(),
        // PubsubMessageToTableRow reports protobuf size, which can be ~1/3rd more efficient than
        // the JSON that actually gets sent over HTTP, so we use 60% of the API limit by default.
        Env.getInt("BATCH_MAX_BYTES", 6_000_000), // HTTP request size limit: 10 MB
        Env.getInt("BATCH_MAX_MESSAGES", 10_000), // Maximum rows per request: 10,000
        Env.getDuration("BATCH_MAX_DELAY", "1s"), // Default 1 second
        Env.getString("OUTPUT_TABLE"), // write messages to OUTPUT_TABLE
        Format.valueOf(Env.getString("OUTPUT_FORMAT", "raw")));

    // read pubsub messages from INPUT_SUBSCRIPTION
    new Pubsub.Read(Env.getString("INPUT_SUBSCRIPTION"), output::apply,
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
