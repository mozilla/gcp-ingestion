package com.mozilla.telemetry.integration;

import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.KeyRing;
import com.google.cloud.kms.v1.LocationName;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test the KeyStore using Google Cloud KMS.
 *
 * <p>First, resources are staged in a local temporary folder. Here, we encrypt
 * the keys using KMS. Then we synchronize the folder with a temporary bucket
 * created in GCS. We programmatically generate a metadata file that we will
 * test. Finally we cover all code paths related to decrypting the private keys.
 */
public class KeyStoreIntegrationTests extends TestWithDeterministicJson {

  private Storage storage;
  private String projectId;
  private String bucket;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  /** Create a storage bucket for metadata and keys. */
  @Before
  public void createBucket() {
    RemoteStorageHelper storageHelper = RemoteStorageHelper.create();
    storage = storageHelper.getOptions().getService();
    projectId = storageHelper.getOptions().getProjectId();
    bucket = RemoteStorageHelper.generateBucketName();
    storage.create(BucketInfo.of(bucket));
  }

  /** Clean up storage resources. */
  @After
  public void deleteBucket() throws Exception {
    RemoteStorageHelper.forceDelete(storage, bucket, 5, TimeUnit.SECONDS);
  }

  private String saltName(String name) {
    String salt = UUID.randomUUID().toString().substring(0, 8);
    return String.format("%s_%s", name, salt);
  }

  @Test
  public void testKeyRing() throws IOException {
    try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {
      // Generate the keyring
      LocationName parent = LocationName.of(projectId, "global");
      String keyRingId = saltName("test");
      KeyRing keyRing = KeyRing.newBuilder().build();
      client.createKeyRing(parent, keyRingId, keyRing);

      // Generate the key
      // Encrypt the data
      // decrypt the data
    }
  }

}
