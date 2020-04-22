package com.mozilla.telemetry.ingestion.sink.io;

import static org.junit.Assert.assertEquals;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.io.BigQuery.BigQueryErrors;
import com.mozilla.telemetry.ingestion.sink.transform.BlobInfoToPubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.BigQueryDataset;
import com.mozilla.telemetry.ingestion.sink.util.GcsBucket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.Rule;
import org.junit.Test;

public class BigQueryLoadIntegrationTest {

  @Rule
  public final GcsBucket gcs = new GcsBucket();

  @Rule
  public final BigQueryDataset bq = new BigQueryDataset();

  private String createTable() {
    final StandardTableDefinition tableDef = StandardTableDefinition
        .of(Schema.of(Field.of("payload", LegacySQLTypeName.STRING)));
    final TableId tableId = TableId.of(bq.project, bq.dataset, "test_load_v1");
    bq.bigquery.create(TableInfo.newBuilder(tableId, tableDef).build());
    return tableId.getProject() + "." + tableId.getDataset() + "." + tableId.getTable();
  }

  private BlobInfo generateBlobId(String outputTable) {
    return BlobInfo
        .newBuilder(BlobId.of(gcs.bucket,
            "OUTPUT_TABLE=" + outputTable + "/" + UUID.randomUUID().toString() + ".ndjson"))
        .build();
  }

  private BlobInfo createBlob(String outputTable, String content) {
    return gcs.storage.create(generateBlobId(outputTable),
        content.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void canLoad() throws Exception {
    final String outputTable = createTable();
    final BlobInfo blobInfo = createBlob(outputTable, "{\"payload\":\"canLoad\"}");
    // load blob
    final BigQuery.Load loader = new BigQuery.Load(bq.bigquery, gcs.storage, 0, 0, Duration.ZERO,
        ForkJoinPool.commonPool(), BigQuery.Load.Delete.onSuccess);
    loader.apply(BlobInfoToPubsubMessage.apply(blobInfo)).join();
    // check result
    final List<String> actual = StreamSupport
        .stream(bq.bigquery.query(QueryJobConfiguration.of("SELECT * FROM `" + outputTable + "`"))
            .iterateAll().spliterator(), false)
        .map(row -> row.get("payload").getStringValue()).collect(Collectors.toList());
    assertEquals(ImmutableList.of("canLoad"), actual);
  }

  @Test
  public void failsOnMissingBlob() {
    final String outputTable = createTable();
    final BlobInfo missingBlob = generateBlobId(outputTable);
    final BlobInfo presentBlob = createBlob(outputTable, "{}");
    // load single batch with missing and present blobs
    final BigQuery.Load loader = new BigQuery.Load(bq.bigquery, gcs.storage, 10, 2,
        Duration.ofMillis(500), ForkJoinPool.commonPool(), BigQuery.Load.Delete.onSuccess);
    final CompletableFuture<Void> missing = loader
        .apply(PubsubMessage.newBuilder().putAttributes("bucket", missingBlob.getBucket())
            .putAttributes("name", missingBlob.getName()).putAttributes("size", "0").build());
    final CompletableFuture<Void> present = loader
        .apply(BlobInfoToPubsubMessage.apply(presentBlob));
    // require single batch of both messages
    assertEquals(ImmutableList.of(2),
        loader.batches.values().stream().map(b -> b.size).collect(Collectors.toList()));
    // require both futures throw BigQueryErrors about missingBlob URI
    Optional<List<String>> expected = Optional.of(ImmutableList
        .of("Not found: URI gs://" + missingBlob.getBucket() + "/" + missingBlob.getName()));
    for (CompletableFuture f : ImmutableList.of(missing, present)) {
      Optional<BigQueryErrors> actual = Optional.empty();
      try {
        f.join();
      } catch (CompletionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof BigQueryErrors) {
          actual = Optional.of((BigQueryErrors) cause);
        } else {
          throw e;
        }
      }
      assertEquals(!f.equals(missing) ? "missingBlob" : "presentBlob", expected, actual
          .map(e -> e.errors.stream().map(BigQueryError::getMessage).collect(Collectors.toList())));
    }
  }
}
