package com.mozilla.telemetry.integration;

import static org.junit.Assert.assertEquals;

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.kms.v1.CryptoKey;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionAlgorithm;
import com.google.cloud.kms.v1.CryptoKeyVersionTemplate;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.EncryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.KeyRing;
import com.google.cloud.kms.v1.KeyRingName;
import com.google.cloud.kms.v1.LocationName;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import com.google.protobuf.ByteString;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.io.IOException;
import java.nio.charset.Charset;
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
public class KeyStoreIntegrationTest extends TestWithDeterministicJson {

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

  @Test
  public void testKeyRing() throws IOException {
    try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {
      // Generate the keyring
      // KeyRings and Keys cannot be deleted, so reuse resources
      String keyRingId = "test-ingestion-beam-integration";
      KeyRingName keyRingName = KeyRingName.of(projectId, "global", keyRingId);

      try {
        client.getKeyRing(keyRingName);
      } catch (NotFoundException e) {
        LocationName parent = LocationName.of(projectId, "global");
        KeyRing request = KeyRing.newBuilder().build();
        client.createKeyRing(parent, keyRingId, request);
      }

      // Get or generate the key
      String keyName = "test";
      String cryptoKeyName = CryptoKeyName.of(projectId, "global", keyRingId, keyName).toString();
      try {
        client.getCryptoKey(cryptoKeyName);
      } catch (NotFoundException e) {
        CryptoKey request = CryptoKey.newBuilder()
            .setPurpose(CryptoKey.CryptoKeyPurpose.ASYMMETRIC_DECRYPT)
            .setVersionTemplate(CryptoKeyVersionTemplate.newBuilder()
                .setAlgorithm(CryptoKeyVersionAlgorithm.RSA_DECRYPT_OAEP_2048_SHA256))
            .build();
        client.createCryptoKey(keyRingName, keyName, request);
      }

      // Encrypt the data
      // TODO: this fails because KMS only supports decryption with asymmetric keys
      ByteString plaintext = ByteString.copyFrom("hello world", Charset.defaultCharset());
      EncryptResponse encrypted = client.encrypt(cryptoKeyName, plaintext);

      // decrypt the data
      DecryptResponse decrypted = client.decrypt(cryptoKeyName, encrypted.getCiphertext());
      assertEquals(plaintext, decrypted.getPlaintext());
    }
  }

}
