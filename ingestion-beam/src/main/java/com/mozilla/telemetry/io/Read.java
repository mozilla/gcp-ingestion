/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.io;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
import com.mozilla.telemetry.heka.HekaIO;
import com.mozilla.telemetry.options.BigQueryReadMethod;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.transforms.MapElementsWithErrors.ToPubsubMessageFrom;
import com.mozilla.telemetry.transforms.WithErrors;
import com.mozilla.telemetry.transforms.WithErrors.Result;
import com.mozilla.telemetry.util.Time;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * Implementations of reading from the sources enumerated in {@link
 * com.mozilla.telemetry.options.InputType}.
 */
public abstract class Read
    extends PTransform<PBegin, WithErrors.Result<PCollection<PubsubMessage>>> {

  /** Implementation of reading from Pub/Sub. */
  public static class PubsubInput extends Read {

    private final ValueProvider<String> subscription;

    public PubsubInput(ValueProvider<String> subscription) {
      this.subscription = subscription;
    }

    @Override
    public Result<PCollection<PubsubMessage>> expand(PBegin input) {
      return input //
          .apply(PubsubIO.readMessagesWithAttributes().fromSubscription(subscription))
          .apply(ToPubsubMessageFrom.identity());
    }
  }

  /** Implementation of reading from line-delimited local or remote files. */
  public static class FileInput extends Read {

    private final ValueProvider<String> fileSpec;
    private final InputFileFormat fileFormat;

    public FileInput(ValueProvider<String> fileSpec, InputFileFormat fileFormat) {
      this.fileSpec = fileSpec;
      this.fileFormat = fileFormat;
    }

    @Override
    public Result<PCollection<PubsubMessage>> expand(PBegin input) {
      return input.apply(TextIO.read().from(fileSpec)).apply(fileFormat.decode());
    }
  }

  /** Implementation of reading from heka blobs stored as files. */
  public static class HekaInput extends Read {

    private final ValueProvider<String> fileSpec;

    public HekaInput(ValueProvider<String> fileSpec) {
      this.fileSpec = fileSpec;
    }

    @Override
    public Result<PCollection<PubsubMessage>> expand(PBegin input) {
      Result<PCollection<List<PubsubMessage>>> result = input
          .apply(FileIO.match().filepattern(fileSpec)).apply(FileIO.readMatches())
          .apply(HekaIO.readFiles());
      result.output().setCoder(ListCoder.of(PubsubMessageWithAttributesCoder.of()));
      return Result.of(result.output().apply(Flatten.iterables()), result.errors());
    }

  }

  /** Implementation of reading from BigQuery. */
  public static class BigQueryInput extends Read {

    private final ValueProvider<String> tableSpec;
    private final BigQueryReadMethod method;
    private final Source source;
    private final String rowRestriction;
    private final List<String> selectedFields;

    public enum Source {
      TABLE, QUERY
    }

    public BigQueryInput(ValueProvider<String> tableSpec, BigQueryReadMethod method, Source source,
        String rowRestriction, List<String> selectedFields) {
      this.tableSpec = tableSpec;
      this.method = method;
      this.source = source;
      this.rowRestriction = rowRestriction;
      this.selectedFields = selectedFields;
    }

    @Override
    public Result<PCollection<PubsubMessage>> expand(PBegin input) {
      BigQueryIO.TypedRead<PubsubMessage> read = BigQueryIO
          .read((SchemaAndRecord schemaAndRecord) -> {
            TableSchema tableSchema = schemaAndRecord.getTableSchema();
            GenericRecord record = schemaAndRecord.getRecord();
            byte[] payload = ((ByteBuffer) record.get("payload")).array();

            // We populate attributes for all simple string and timestamp fields, which is complete
            // for raw and error tables; decoded payload tables have the nested metadata object also
            // encoded in the payload, so we can safely drop the metadata object here and rely on
            // ParsePayload to parse attributes from it.
            Map<String, String> attributes = new HashMap<>();
            tableSchema.getFields().stream() //
                .filter(f -> !"REPEATED".equals(f.getMode())) //
                .forEach(f -> {
                  Object value = record.get(f.getName());
                  if (value != null) {
                    switch (f.getType()) {
                      case "TIMESTAMP":
                        attributes.put(f.getName(), Time.epochMicrosToTimestamp((Long) value));
                        break;
                      case "STRING":
                      case "INTEGER":
                      case "INT64":
                        attributes.put(f.getName(), value.toString());
                        break;
                      // Ignore any other types (only the payload BYTES field should hit this).
                      default:
                        break;
                    }
                  }
                });
            return new PubsubMessage(payload, attributes);
          }) //
          .withCoder(PubsubMessageWithAttributesCoder.of()) //
          .withTemplateCompatibility() //
          .withoutValidation() //
          .withMethod(method.method);
      switch (source) {
        case TABLE:
          read = read.from(tableSpec);
          break;
        default:
        case QUERY:
          read = read.fromQuery(tableSpec).usingStandardSql();
      }
      if (method == BigQueryReadMethod.storageapi) {
        TableReadOptions.Builder builder = TableReadOptions.newBuilder();
        if (rowRestriction != null) {
          builder.setRowRestriction(rowRestriction);
        }
        if (selectedFields != null && selectedFields.size() > 0) {
          builder.addAllSelectedFields(selectedFields);
        }
        read = read.withReadOptions(builder.build());
      }
      return input.apply(read).apply(ToPubsubMessageFrom.identity());
    }
  }
}
