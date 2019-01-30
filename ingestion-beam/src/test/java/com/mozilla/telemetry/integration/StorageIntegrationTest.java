/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.integration;

import static com.mozilla.telemetry.matchers.Lines.matchesInAnyOrder;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import com.mozilla.telemetry.Decoder;
import com.mozilla.telemetry.matchers.Lines;
import com.mozilla.telemetry.rules.RedisServer;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StorageIntegrationTest {

  private Storage storage;
  private String projectId;
  private String bucket;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public final RedisServer redis = new RedisServer();

  /** Find credentials in the environment and create a bucket in GCS. */
  @Before
  public void createBucket() {
    RemoteStorageHelper storageHelper = RemoteStorageHelper.create();
    storage = storageHelper.getOptions().getService();
    projectId = storageHelper.getOptions().getProjectId();
    bucket = RemoteStorageHelper.generateBucketName();
    storage.create(BucketInfo.of(bucket));
  }

  /** Clean up all GCS resources we created. */
  @After
  public void deleteBucket() throws Exception {
    RemoteStorageHelper.forceDelete(storage, bucket, 5, TimeUnit.SECONDS);
  }

  /** Make serialization of attributes map deterministic for these tests. */
  @BeforeClass
  public static void setUp() throws Exception {
    Field mapperField = Json.class.getDeclaredField("MAPPER");
    mapperField.setAccessible(true);
    ObjectMapper mapper = (ObjectMapper) mapperField.get(null);
    mapper.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
  }

  /** Reset the ObjectMapper configurations we changed. */
  @AfterClass
  public static void tearDown() throws Exception {
    Field mapperField = Json.class.getDeclaredField("MAPPER");
    mapperField.setAccessible(true);
    ObjectMapper mapper = (ObjectMapper) mapperField.get(null);
    mapper.disable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
  }

  /**
   * This test uses the same general options and input as DecoderMainTest, except that it's
   * configured to read from copies on GCS and write output to GCS.
   * The intention is for this test to remain as identical to DecoderMainTest as possible.
   */
  @Test
  public void testDecoderWithRemoteInputAndOutput() throws Exception {
    String gcsPath = "gs://" + bucket;
    String resourceDir = "src/test/resources/testdata/decoder-integration";
    String input = gcsPath + "/in/*-input.ndjson";
    String output = gcsPath + "/out/out";
    String errorOutput = gcsPath + "/error/error";

    uploadInputFile(resourceDir + "/valid-input.ndjson");
    uploadInputFile(resourceDir + "/error-input.ndjson");

    Decoder.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file", "--output=" + output,
        "--outputFileCompression=UNCOMPRESSED", "--errorOutputFileCompression=UNCOMPRESSED",
        "--errorOutputType=file", "--errorOutput=" + errorOutput, "--includeStackTrace=false",
        "--geoCityDatabase=GeoLite2-City.mmdb", "--schemasLocation=schemas.tar.gz",
        "--seenMessagesSource=none", "--redisUri=" + redis.uri });

    tempFolder.newFolder("out");
    tempFolder.newFolder("error");
    downloadOutputFiles("out/");
    downloadOutputFiles("error/");
    String localOutputDir = tempFolder.getRoot().getPath();

    List<String> outputLines = Lines.files(localOutputDir + "/out/*.ndjson");
    List<String> expectedOutputLines = Lines.files(resourceDir + "/output.ndjson");
    assertThat("Main output differed from expectation", outputLines,
        matchesInAnyOrder(expectedOutputLines));

    List<String> errorOutputLines = Lines.files(localOutputDir + "/error/*.ndjson");
    List<String> expectedErrorOutputLines = Lines.files(resourceDir + "/error-output.ndjson");
    assertThat("Error output differed from expectation", errorOutputLines,
        matchesInAnyOrder(expectedErrorOutputLines));
  }

  @Test
  public void testCompileDataflowTemplate() throws Exception {
    String gcsPath = "gs://" + bucket;

    Decoder.main(new String[] { "--runner=Dataflow", "--project=" + projectId,
        "--templateLocation=" + gcsPath + "/templates/TestTemplate",
        "--stagingLocation=" + gcsPath + "/temp/staging", "--inputFileFormat=json",
        "--inputType=file", "--outputFileFormat=json", "--outputType=file",
        "--errorOutputType=file", "--geoCityDatabase=GeoLite2-City.mmdb",
        "--schemasLocation=schemas.tar.gz", "--seenMessagesSource=none" });

  }

  private void uploadInputFile(String localPath) throws IOException {
    String[] pathElements = localPath.split("/");
    String remotePath = "in/" + pathElements[pathElements.length - 1];
    byte[] content = Files.readAllBytes(Paths.get(localPath));
    storage.create(BlobInfo.newBuilder(BlobId.of(bucket, remotePath)).build(), content);
  }

  private void downloadOutputFiles(String prefix) throws IOException {
    Page<Blob> blobs = storage.list(bucket, BlobListOption.prefix(prefix));
    for (Blob blob : blobs.iterateAll()) {
      blob.downloadTo(Paths.get(tempFolder.getRoot().getPath(), blob.getName()));
    }
  }

}
