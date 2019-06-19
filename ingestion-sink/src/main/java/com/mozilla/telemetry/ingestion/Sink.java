/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.cloud.bigquery.BigQueryOptions;
import com.mozilla.telemetry.ingestion.io.BigQuery;
import com.mozilla.telemetry.ingestion.io.Gcs;
import com.mozilla.telemetry.ingestion.io.Pubsub;
import com.mozilla.telemetry.ingestion.transform.FileGroupByKey;
import com.mozilla.telemetry.ingestion.transform.RouteMessage;
import com.mozilla.telemetry.ingestion.transform.PubsubMessageToTableRow;
import com.mozilla.telemetry.ingestion.util.Env;
import com.mozilla.telemetry.ingestion.util.Json;
import com.mozilla.telemetry.ingestion.util.KV;
import java.util.concurrent.CompletableFuture;

public class Sink {

  private Sink() {
  }

  public static void main(String[] args) {
    if (Env.optString("OUTPUT_BUCKET").isPresent()) {
      // output messages to OUTPUT_BUCKET
      Gcs.Write gcsWrite = new Gcs.Write(Env.getString("OUTPUT_BUCKET"));

      // output gcs object paths to OUTPUT_TOPIC
      Pubsub.Write pubsubWrite = new Pubsub.Write(Env.getString("OUTPUT_TOPIC"),
          Env.getInteger("EXECUTOR_THREADS", 1));

      // write newline delimited strings to files by key
      FileGroupByKey<String, String> output = new FileGroupByKey<>(
          Env.getInteger("OUTPUT_BATCH_MAX_BYTES", (int) 1e11), // default 100 GB
          Env.getDuration("OUTPUT_BATCH_MAX_DELAY", "PT10m"), // default 10 min
          Env.getInteger("OUTPUT_BATCH_MAX_MESSAGES", (int) 1e9), // default 1bn messages
          kv -> CompletableFuture.supplyAsync(() -> kv) // start new future with key-file pair
              .thenApplyAsync(gcsWrite) // upload file and return gcs object path
              // encode gcs object path in pubsub message
              .thenApplyAsync(s -> PubsubMessage.newBuilder() //
                  .setData(ByteString.copyFrom(s.getBytes())).build()) //
              .thenComposeAsync(pubsubWrite)); // publish to pubsub

      // read pubsub messages from INPUT_SUBSCRIPTION
      new Pubsub.Read(
          Env.getString("INPUT_SUBSCRIPTION"),
          message -> CompletableFuture.supplyAsync(() -> message) // start new future with message
              .thenApplyAsync(RouteMessage::apply) // determine output path of message
              .thenApplyAsync(kv -> new KV<>(kv.key, Json.asString(kv.value))) // encode message as json
              .thenComposeAsync(output)) // output message
          .run(); // run pubsub consumer
    } else {
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
}
