package com.mozilla.telemetry.integration;

import static org.junit.Assert.assertEquals;

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.kms.v1.AsymmetricDecryptResponse;
import com.google.cloud.kms.v1.CryptoKey;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionAlgorithm;
import com.google.cloud.kms.v1.CryptoKeyVersionName;
import com.google.cloud.kms.v1.CryptoKeyVersionTemplate;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.KeyRing;
import com.google.cloud.kms.v1.KeyRingName;
import com.google.cloud.kms.v1.LocationName;
import com.google.cloud.kms.v1.PublicKey;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import com.google.protobuf.ByteString;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.encodings.OAEPEncoding;
import org.bouncycastle.crypto.engines.RSAEngine;
import org.bouncycastle.crypto.params.AsymmetricKeyParameter;
import org.bouncycastle.crypto.util.PublicKeyFactory;
import org.bouncycastle.util.io.pem.PemReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test the KeyStore using Google Cloud KMS.
 *
 * <p>
 * First, resources are staged in a local temporary folder. Here, we encrypt the
 * keys using KMS. Then we synchronize the folder with a temporary bucket
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

  /**
   * https://stackoverflow.com/questions/17110217/is-rsa-pkcs1-oaep-padding-supported-in-bouncycastle
   * https://www.bouncycastle.org/docs/docs1.5on/index.html
   */
  private byte[] encrypt(PublicKey publicKey, byte[] data)
      throws IOException, InvalidCipherTextException {
    try (InputStream inputStream = IOUtils.toInputStream(publicKey.getPem(), "UTF-8");
        PemReader pemReader = new PemReader(new InputStreamReader(inputStream))) {
      AsymmetricKeyParameter param = PublicKeyFactory
          .createKey(pemReader.readPemObject().getContent());
      OAEPEncoding cipher = new OAEPEncoding(new RSAEngine(), new SHA256Digest());
      cipher.init(true, param);
      return cipher.processBlock(data, 0, data.length);
    }
  }

  @Test
  public void testKeyRing() throws IOException, InvalidCipherTextException {
    try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {
      // Generate the keyring
      // KeyRings and Keys cannot be deleted, so reuse resources
      String keyRingId = "test-ingestion-beam-integration";
      String keyName = "testing-key";

      KeyRingName keyRingName = KeyRingName.of(projectId, "global", keyRingId);

      try {
        client.getKeyRing(keyRingName);
      } catch (NotFoundException e) {
        LocationName parent = LocationName.of(projectId, "global");
        KeyRing request = KeyRing.newBuilder().build();
        client.createKeyRing(parent, keyRingId, request);
      }

      try {
        String resourceId = CryptoKeyName.of(projectId, "global", keyRingId, keyName).toString();
        client.getCryptoKey(resourceId);
      } catch (NotFoundException e) {
        CryptoKey request = CryptoKey.newBuilder()
            .setPurpose(CryptoKey.CryptoKeyPurpose.ASYMMETRIC_DECRYPT)
            .setVersionTemplate(CryptoKeyVersionTemplate.newBuilder()
                .setAlgorithm(CryptoKeyVersionAlgorithm.RSA_DECRYPT_OAEP_2048_SHA256))
            .build();
        client.createCryptoKey(keyRingName, keyName, request);
      }

      byte[] plaintext = "hello world".getBytes();
      String resourceId = CryptoKeyVersionName.of(projectId, "global", keyRingId, keyName, "1")
          .toString();
      // Public key must be versioned, so always work with the first version of the key
      // This requires viewPublicKey permission, not included as KMS admin
      PublicKey publicKey = client.getPublicKey(resourceId);
      byte[] ciphertext = encrypt(publicKey, plaintext);

      // decrypt the data
      AsymmetricDecryptResponse decrypted = client.asymmetricDecrypt(resourceId,
          ByteString.copyFrom(ciphertext));
      assertEquals("hello world", decrypted.getPlaintext().toStringUtf8());
    }
  }

}
