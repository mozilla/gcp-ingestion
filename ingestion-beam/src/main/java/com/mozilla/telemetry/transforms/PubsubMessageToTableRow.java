package com.mozilla.telemetry.transforms;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.TableId;
import com.mozilla.telemetry.ingestion.core.transform.PubsubMessageToObjectNode;
import com.mozilla.telemetry.util.BeamFileInputStream;
import com.mozilla.telemetry.util.GzipUtil;
import com.mozilla.telemetry.util.Json;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.values.KV;

/**
 * Parses JSON payloads using Google's JSON API model library, emitting a BigQuery-specific
 * TableRow.
 *
 * <p>We also perform some manipulation of the parsed JSON to match details of our table
 * schemas in BigQuery.
 */
public class PubsubMessageToTableRow implements Serializable {

  public static PubsubMessageToTableRow of(List<String> strictSchemaDocTypes,
      String schemasLocation, TableRowFormat tableRowFormat) {
    return new PubsubMessageToTableRow(strictSchemaDocTypes, schemasLocation, tableRowFormat);
  }

  public enum TableRowFormat {
    raw, decoded, payload
  }

  private final List<String> strictSchemaDocTypes;
  private final String schemasLocation;
  private final TableRowFormat tableRowFormat;

  // We'll instantiate these on first use.
  private transient PubsubMessageToObjectNode format;

  private PubsubMessageToTableRow(List<String> strictSchemaDocTypes, String schemasLocation,
      TableRowFormat tableRowFormat) {
    this.strictSchemaDocTypes = strictSchemaDocTypes;
    this.schemasLocation = schemasLocation;
    this.tableRowFormat = tableRowFormat;
  }

  private PubsubMessageToObjectNode createFormat() {
    switch (tableRowFormat) {
      case raw:
        return PubsubMessageToObjectNode.Raw.of();
      case decoded:
        return PubsubMessageToObjectNode.Decoded.of();
      case payload:
      default:
        return PayloadWithBeam.of(strictSchemaDocTypes, schemasLocation);
    }
  }

  /**
   * Given a KV containing a destination and a message, return the message content as a {@link
   * TableRow} ready to pass to {@link org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO}.
   */
  public TableRow kvToTableRow(KV<TableDestination, PubsubMessage> kv) {
    if (format == null) {
      format = createFormat();
    }
    final TableReference ref = kv.getKey().getTableReference();
    final TableId tableId = TableId.of(ref.getProjectId(), ref.getDatasetId(), ref.getTableId());
    final PubsubMessage message = kv.getValue();
    return Json.asTableRow(format.apply(tableId, message.getAttributeMap(), message.getPayload()));
  }

  private static class PayloadWithBeam extends PubsubMessageToObjectNode.Payload {

    /**
     * Measure transform with Beam metrics.
     */
    public static PayloadWithBeam of(List<String> strictSchemaDocTypes, String schemasLocation) {
      return new PayloadWithBeam(strictSchemaDocTypes, schemasLocation);
    }

    private PayloadWithBeam(List<String> strictSchemaDocTypes, String schemasLocation) {
      super(strictSchemaDocTypes, schemasLocation, BeamFileInputStream::open);
    }

    // Metrics
    private static final Counter coercedToInt = Metrics.counter(PubsubMessageToTableRow.class,
        "coerced_to_int");
    private static final Counter notCoercedToInt = Metrics.counter(PubsubMessageToTableRow.class,
        "not_coerced_to_int");
    private static final Counter notCoercedToBool = Metrics.counter(PubsubMessageToTableRow.class,
        "not_coerced_to_bool");
    private static final Counter invalidHistogramType = Metrics
        .counter(PubsubMessageToTableRow.class, "invalid_histogram_type");
    private static final Counter invalidHistogramSum = Metrics
        .counter(PubsubMessageToTableRow.class, "invalid_histogram_sum");
    private static final Counter invalidHistogramUseCounter = Metrics
        .counter(PubsubMessageToTableRow.class, "invalid_histogram_use_counter");
    private static final Counter invalidHistogramRange = Metrics
        .counter(PubsubMessageToTableRow.class, "invalid_histogram_range");

    /** measure rate of CoercedToInt. */
    @Override
    protected void incrementCoercedToInt() {
      coercedToInt.inc();
    }

    /** measure rate of NotCoercedToInt. */
    @Override
    protected void incrementNotCoercedToInt() {
      notCoercedToInt.inc();
    }

    /** measure rate of NotCoercedToBool. */
    @Override
    protected void incrementNotCoercedToBool() {
      notCoercedToBool.inc();
    }

    /** measure rate of InvalidHistogramType. */
    @Override
    protected void incrementInvalidHistogramType() {
      invalidHistogramType.inc();
    }

    /** measure rate of InvalidHistogramSum. */
    @Override
    protected void incrementInvalidHistogramSum() {
      invalidHistogramSum.inc();
    }

    /** measure rate of InvalidHistogramUseCounter. */
    @Override
    protected void incrementInvalidHistogramUseCounter() {
      invalidHistogramUseCounter.inc();
    }

    /** measure rate of InvalidHistogramRange. */
    @Override
    protected void incrementInvalidHistogramRange() {
      invalidHistogramRange.inc();
    }

    @Override
    public ObjectNode apply(TableId tableId, Map<String, String> attributes, byte[] data) {
      // attempt to decompress data that may have been compressed to minimize shuffle size
      return super.apply(tableId, attributes, GzipUtil.maybeDecompress(data));
    }
  }
}
