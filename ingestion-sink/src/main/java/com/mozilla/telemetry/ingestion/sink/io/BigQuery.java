/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.sink.io;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.ingestion.core.util.Json;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToObjectNode;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToObjectNode.Format;
import com.mozilla.telemetry.ingestion.sink.util.BatchWrite;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQuery {

  private BigQuery() {
  }

  @VisibleForTesting
  static class WriteErrors extends RuntimeException {

    public final List<BigQueryError> errors;

    private WriteErrors(List<BigQueryError> errors) {
      this.errors = errors;
    }
  }

  public static class Write
      extends BatchWrite<PubsubMessage, PubsubMessage, TableId, InsertAllResponse> {

    private static final Logger LOG = LoggerFactory.getLogger(Write.class);

    private final com.google.cloud.bigquery.BigQuery bigQuery;
    private final PubsubMessageToObjectNode encoder;

    public Write(com.google.cloud.bigquery.BigQuery bigQuery, long maxBytes, int maxMessages,
        Duration maxDelay, String batchKeyTemplate, Format format) {
      super(maxBytes, maxMessages, maxDelay, batchKeyTemplate);
      this.bigQuery = bigQuery;
      this.encoder = new PubsubMessageToObjectNode(format);
    }

    @Override
    protected TableId getBatchKey(PubsubMessage input) {
      String batchKey = batchKeyTemplate.apply(input).replaceAll(":", ".");
      final String[] tableSpecParts = batchKey.split("\\.", 3);
      if (tableSpecParts.length == 3) {
        return TableId.of(tableSpecParts[0], tableSpecParts[1], tableSpecParts[2]);
      } else if (tableSpecParts.length == 2) {
        return TableId.of(tableSpecParts[0], tableSpecParts[1]);
      } else {
        throw new IllegalArgumentException("Could not determine dataset from the configured"
            + " BigQuery output template: " + batchKeyTemplate.template);
      }
    }

    @Override
    protected PubsubMessage encodeInput(PubsubMessage input) {
      return input;
    }

    @Override
    protected Batch getBatch(TableId tableId) {
      return new Batch(tableId);
    }

    @VisibleForTesting
    class Batch extends BatchWrite<PubsubMessage, PubsubMessage, TableId, InsertAllResponse>.Batch {

      @VisibleForTesting
      final InsertAllRequest.Builder builder;

      private Batch(TableId tableId) {
        super();
        builder = InsertAllRequest.newBuilder(tableId)
            // ignore row values for columns not present in the table
            .setIgnoreUnknownValues(true)
            // insert all valid rows when invalid rows are present in the request
            .setSkipInvalidRows(true);
      }

      @Override
      protected synchronized CompletableFuture<InsertAllResponse> close(Void ignore) {
        return CompletableFuture.completedFuture(bigQuery.insertAll(builder.build()));
      }

      @Override
      protected synchronized void write(PubsubMessage input) {
        Map<String, Object> content = Json.asMap(encoder.apply(input));
        Optional.ofNullable(input.getAttributesOrDefault(Attribute.DOCUMENT_ID, null))
            .map(id -> builder.addRow(id, content)).orElseGet(() -> builder.addRow(content));
      }

      @Override
      protected long getByteSize(PubsubMessage input) {
        // plus one for a comma between messages in the HTTP request
        return input.getSerializedSize() + 1;
      }

      @Override
      protected void checkResultFor(InsertAllResponse batchResult, int index) {
        Optional.ofNullable(batchResult.getErrorsFor(index)).filter(errors -> !errors.isEmpty())
            .ifPresent(errors -> {
              LOG.warn("Write errors: " + errors.toString());
              throw new WriteErrors(errors);
            });
      }
    }
  }
}
