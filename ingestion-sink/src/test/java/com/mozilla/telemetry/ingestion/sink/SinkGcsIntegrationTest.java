package com.mozilla.telemetry.ingestion.sink;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.Assert.assertEquals;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.BoundedSink;
import com.mozilla.telemetry.ingestion.sink.util.GcsBucket;
import com.mozilla.telemetry.ingestion.sink.util.SinglePubsubTopic;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class SinkGcsIntegrationTest {

  @Rule
  public final SinglePubsubTopic pubsub = new SinglePubsubTopic();

  @Rule
  public final GcsBucket gcs = new GcsBucket();

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Before
  public void unregisterStackdriver() {
    // unregister stackdriver stats exporter in case a previous test already registered one.
    StackdriverStatsExporter.unregister();
  }

  @Test
  public void canSinkRawMessages() throws IOException {
    String submissionTimestamp = ZonedDateTime.now(ZoneOffset.UTC)
        .format(DateTimeFormatter.ISO_DATE_TIME);
    List<String> versions = ImmutableList.of("1", "2");
    versions.forEach(version -> pubsub.publish(PubsubMessage.newBuilder()
        .setData(ByteString.copyFrom("test".getBytes(StandardCharsets.UTF_8)))
        .putAttributes("document_namespace", "namespace").putAttributes("document_type", "type")
        .putAttributes("document_version", version)
        .putAttributes("submission_timestamp", submissionTimestamp).build()));

    environmentVariables.set("INPUT_SUBSCRIPTION", pubsub.getSubscription());
    environmentVariables.set("BATCH_MAX_DELAY", "0s");
    environmentVariables.set("OUTPUT_BUCKET",
        gcs.bucket + "/${document_namespace}/${document_type}_"
            + "v${document_version}/${submission_date}/${submission_hour}/");

    BoundedSink.run(versions.size(), 30);

    List<Blob> actual = new LinkedList<>();
    gcs.storage.list(gcs.bucket, Storage.BlobListOption.prefix("namespace/type_")).iterateAll()
        .forEach(actual::add);

    List<String> expectedNamePrefixes = ImmutableList.of(
        "namespace/type_v1/" + submissionTimestamp.substring(0, 10) + "/"
            + submissionTimestamp.substring(11, 13),
        "namespace/type_v2/" + submissionTimestamp.substring(0, 10) + "/"
            + submissionTimestamp.substring(11, 13));
    assertEquals(expectedNamePrefixes, actual.stream().map(blob -> blob.getName().substring(0, 31))
        .sorted().collect(Collectors.toList()));

    actual.forEach(blob -> assertThat(blob.getName(),
        matchesPattern("namespace/type_v[12]/[0-9-]{10}/[0-9]{2}/[a-f0-9-]{36}\\.ndjson")));

    List<List<String>> expectedRows = ImmutableList.of(
        ImmutableList.of("\"document_namespace\":\"namespace\"", "\"document_type\":\"type\"",
            "\"document_version\":\"1\"", "\"payload\":\"dGVzdA==\"",
            "\"submission_timestamp\":\"" + submissionTimestamp + "\""),
        ImmutableList.of("\"document_namespace\":\"namespace\"", "\"document_type\":\"type\"",
            "\"document_version\":\"2\"", "\"payload\":\"dGVzdA==\"",
            "\"submission_timestamp\":\"" + submissionTimestamp + "\""));
    assertEquals(expectedRows,
        actual.stream().sorted(Comparator.comparing(Blob::getName))
            .map(blob -> Arrays
                .stream(new String(gcs.storage.readAllBytes(blob.getBlobId())).split("[{},\n]"))
                .filter(s -> !s.isEmpty()).sorted().collect(Collectors.toList()))
            .collect(Collectors.toList()));
  }
}
