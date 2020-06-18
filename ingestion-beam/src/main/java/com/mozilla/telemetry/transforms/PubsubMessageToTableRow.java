package com.mozilla.telemetry.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.mozilla.telemetry.ingestion.core.transform.PubsubMessageToObjectNode;
import com.mozilla.telemetry.util.BeamFileInputStream;
import com.mozilla.telemetry.util.Json;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.KV;

/**
 * Parses JSON payloads using Google's JSON API model library, emitting a BigQuery-specific
 * TableRow.
 *
 * <p>We also perform some manipulation of the parsed JSON to match details of our table
 * schemas in BigQuery.
 */
public class PubsubMessageToTableRow implements Serializable {

  public static PubsubMessageToTableRow of(ValueProvider<List<String>> strictSchemaDocTypes,
      ValueProvider<String> schemasLocation, ValueProvider<TableRowFormat> tableRowFormat) {
    return new PubsubMessageToTableRow(strictSchemaDocTypes, schemasLocation, tableRowFormat);
  }

  public enum TableRowFormat {
    raw, decoded, payload
  }

  private final ValueProvider<List<String>> strictSchemaDocTypes;
  private final ValueProvider<String> schemasLocation;
  private final ValueProvider<TableRowFormat> tableRowFormat;

  // We'll instantiate these on first use.
  private transient PubsubMessageToObjectNode format;

  private PubsubMessageToTableRow(ValueProvider<List<String>> strictSchemaDocTypes,
      ValueProvider<String> schemasLocation, ValueProvider<TableRowFormat> tableRowFormat) {
    this.strictSchemaDocTypes = strictSchemaDocTypes;
    this.schemasLocation = schemasLocation;
    this.tableRowFormat = tableRowFormat;
  }

  private PubsubMessageToObjectNode createFormat() {
    switch (tableRowFormat.get()) {
      case raw:
        return PubsubMessageToObjectNode.Raw.of();
      case decoded:
        return PubsubMessageToObjectNode.Decoded.of();
      case payload:
      default:
        return PayloadWithBeamMetrics.of(strictSchemaDocTypes.get(), schemasLocation.get(), null)
            .withOpenCensusMetrics();
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
    PubsubMessage message = kv.getValue();
    return Json.asTableRow(format.apply(message.getAttributeMap(), message.getPayload()));
  }

  public static class PayloadWithBeamMetrics extends PubsubMessageToObjectNode.Payload {

    /**
     * Measure transform with Beam metrics.
     */
    public static PayloadWithBeamMetrics of(List<String> strictSchemaDocTypes,
        String schemasLocation) {
      return new PayloadWithBeamMetrics(strictSchemaDocTypes, schemasLocation);
    }

    private PayloadWithBeamMetrics(List<String> strictSchemaDocTypes, String schemasLocation) {
      super(strictSchemaDocTypes, schemasLocation, BeamFileInputStream::open);
    }

    // Metrics
    private static final Counter coercedToInt = Metrics.counter(PubsubMessageToTableRow.class,
        "coerced_to_int");
    private static final Counter notCoercedToInt = Metrics.counter(PubsubMessageToTableRow.class,
        "not_coerced_to_int");
    private static final Counter notCoercedToBool = Metrics.counter(PubsubMessageToTableRow.class,
        "not_coerced_to_bool");

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
  }
}
