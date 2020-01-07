package com.mozilla.telemetry.ingestion.sink.util;

import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * JUnit Rule for reusing code to interact with Cloud Storage.
 */
public class GcsBucket extends TestWatcher {

  public Storage storage;
  public String bucket;

  /** Find credentials in the environment and create a bucket in GCS. */
  @Override
  protected void starting(Description description) {
    RemoteStorageHelper storageHelper = RemoteStorageHelper.create();
    storage = storageHelper.getOptions().getService();
    bucket = RemoteStorageHelper.generateBucketName();
    storage.create(BucketInfo.newBuilder(bucket).build());
  }

  /** Remove all resources we created in GCS. */
  @Override
  protected void finished(Description description) {
    RemoteStorageHelper.forceDelete(storage, bucket);
  }
}
