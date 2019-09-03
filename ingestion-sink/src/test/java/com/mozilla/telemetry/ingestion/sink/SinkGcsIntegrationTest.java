/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.sink;

import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.io.Pubsub;
import com.mozilla.telemetry.ingestion.sink.util.TestWithSinglePubsubTopic;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.Timeout;

public class SinkGcsIntegrationTest extends TestWithSinglePubsubTopic {

  private Storage storage;
  private String bucket;

  /** Find credentials in the environment and create a bucket in GCS. */
  @Before
  public void createGcsBucket() {
    RemoteStorageHelper storageHelper = RemoteStorageHelper.create();
    storage = storageHelper.getOptions().getService();
    bucket = RemoteStorageHelper.generateBucketName();
    storage.create(BucketInfo.newBuilder(bucket).build());
  }

  /** Remove all resources we created in GCS. */
  @After
  public void deleteGcsBucket() {
    RemoteStorageHelper.forceDelete(storage, bucket);
  }

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Rule
  public final Timeout timeout = new Timeout(30, TimeUnit.SECONDS);

  @Test
  public void canSinkRawMessages() {
    String submissionTimestamp = ZonedDateTime.now(ZoneOffset.UTC)
        .format(DateTimeFormatter.ISO_DATE_TIME);
    List<String> versions = ImmutableList.of("1", "2");
    versions.forEach(version -> publish(
        PubsubMessage.newBuilder().setData(ByteString.copyFrom("test".getBytes()))
            .putAttributes("document_namespace", "namespace").putAttributes("document_type", "type")
            .putAttributes("document_version", version)
            .putAttributes("submission_timestamp", submissionTimestamp).build()));

    environmentVariables.set("INPUT_SUBSCRIPTION", getSubscription());
    environmentVariables.set("BATCH_MAX_DELAY", "0.001s");
    environmentVariables.set("OUTPUT_BUCKET", bucket + "/${document_namespace}/${document_type}_"
        + "v${document_version}/${submission_date}/${submission_hour}/");

    Pubsub.Read pubsubRead = Sink.main();
    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    CompletableFuture<Void> main = CompletableFuture.runAsync(pubsubRead::run);

    AtomicReference<List<Blob>> actual = new AtomicReference<>();
    do {
      actual.set(new LinkedList<>());
      storage.list(bucket, Storage.BlobListOption.prefix("namespace/type_")).iterateAll()
          .forEach(blob -> actual.get().add(blob));
    } while (actual.get().size() < versions.size());
    assertEquals(versions.size(), actual.get().size());

    pubsubRead.subscriber.stopAsync();
    main.join();

    List<String> expectedNamePrefixes = ImmutableList.of(
        "namespace/type_v1/" + submissionTimestamp.substring(0, 10) + "/"
            + submissionTimestamp.substring(11, 13),
        "namespace/type_v2/" + submissionTimestamp.substring(0, 10) + "/"
            + submissionTimestamp.substring(11, 13));
    assertEquals(expectedNamePrefixes, actual.get().stream()
        .map(blob -> blob.getName().substring(0, 31)).sorted().collect(Collectors.toList()));

    actual.get().forEach(blob -> assertThat(blob.getName(),
        matchesPattern("namespace/type_v[12]/[0-9-]{10}/[0-9]{2}/[a-f0-9-]{36}\\.ndjson")));

    List<List<String>> expectedRows = ImmutableList.of(
        ImmutableList.of("\"document_namespace\":\"namespace\"", "\"document_type\":\"type\"",
            "\"document_version\":\"1\"", "\"payload\":\"dGVzdA==\"",
            "\"submission_timestamp\":\"" + submissionTimestamp + "\""),
        ImmutableList.of("\"document_namespace\":\"namespace\"", "\"document_type\":\"type\"",
            "\"document_version\":\"2\"", "\"payload\":\"dGVzdA==\"",
            "\"submission_timestamp\":\"" + submissionTimestamp + "\""));
    assertEquals(expectedRows,
        actual.get().stream().sorted(Comparator.comparing(Blob::getName))
            .map(blob -> Arrays
                .stream(new String(storage.readAllBytes(blob.getBlobId())).split("[{},\n]"))
                .filter(s -> !s.isEmpty()).sorted().collect(Collectors.toList()))
            .collect(Collectors.toList()));
  }
}
