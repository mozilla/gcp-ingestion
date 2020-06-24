package com.mozilla.telemetry.ingestion.core.schema;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.mozilla.telemetry.ingestion.core.util.IOFunction;
import com.mozilla.telemetry.ingestion.core.util.Json;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.io.IOUtils;

public class BigQuerySchemaStore extends SchemaStore<Schema> {

  /**
   * Returns a SchemaStore based on the contents of the archive at schemasLocation.
   */
  public static BigQuerySchemaStore of(String schemasLocation,
      IOFunction<String, InputStream> open) {
    if (schemasLocation != null) {
      return new BigQuerySchemaStore(schemasLocation, open);
    } else {
      return new FromApi();
    }
  }

  protected BigQuerySchemaStore(String schemasLocation, IOFunction<String, InputStream> open) {
    super(schemasLocation, open);
  }

  @Override
  protected String schemaSuffix() {
    return ".bq";
  }

  @Override
  protected Schema loadSchemaFromArchive(ArchiveInputStream archive) throws IOException {
    byte[] bytes = IOUtils.toByteArray(archive);
    return Json.readBigQuerySchema(bytes);
  }

  // Overridden via FromApi when schemasLocation is null
  public Schema getSchema(TableId tableId, Map<String, String> attributes) {
    return getSchema(attributes);
  }

  /**
   * Implement BigQuerySchemaStore without schemasLocation by reading schemas from BigQuery API.
   */
  private static class FromApi extends BigQuerySchemaStore {

    private FromApi() {
      super(null, null);
    }

    @Override
    protected void ensureSchemasLoaded() {
      // load schemas on demand
    }

    @Override
    public Schema getSchema(String path) {
      // can't find schema without tableId
      throw SchemaNotFoundException.forName(path);
    }

    // We have hit rate limiting issues that have sent valid data to error output, so we make the
    // retry settings a bit more generous; see https://github.com/mozilla/gcp-ingestion/issues/651
    private static final RetrySettings RETRY_SETTINGS = ServiceOptions //
        .getDefaultRetrySettings().toBuilder() //
        .setMaxAttempts(12) // Defaults to 6
        .setTotalTimeout(org.threeten.bp.Duration.ofSeconds(120)) // Defaults to 50 seconds
        .build();
    private transient Cache<TableId, Schema> tableSchemaCache;
    private transient BigQuery bqService;

    @Override
    public Schema getSchema(TableId tableId, Map<String, String> attributes) {
      if (tableId == null) {
        // Always throws SchemaNotFoundException
        return getSchema(attributes);
      }
      if (tableSchemaCache == null) {
        // We need to be very careful about settings for the cache here. We have had significant
        // issues in the past due to exceeding limits on BigQuery API requests; see
        // https://bugzilla.mozilla.org/show_bug.cgi?id=1623000
        tableSchemaCache = CacheBuilder.newBuilder().expireAfterWrite(Duration.ofMinutes(10))
            .build();
      }
      if (bqService == null) {
        bqService = BigQueryOptions.newBuilder().setProjectId(tableId.getProject())
            .setRetrySettings(RETRY_SETTINGS).build().getService();
      }
      try {
        return Optional.of(tableSchemaCache.get(tableId, () -> {
          Table table = bqService.getTable(tableId);
          if (table != null) {
            return table.getDefinition().getSchema();
          } else {
            return null;
          }
        })).orElseThrow(() -> SchemaNotFoundException.forName(tableId.toString()));
      } catch (ExecutionException e) {
        throw new UncheckedExecutionException(e.getCause());
      }
    }
  }
}
