package com.mozilla.telemetry.integration;

import static org.junit.Assert.assertEquals;

import com.google.api.gax.rpc.NotFoundException;
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
 * <p>First, resources are staged in a local temporary folder. Here, we encrypt the
 * keys using KMS. Then we synchronize the folder with a temporary bucket
 * created in GCS. We programmatically generate a metadata file that we will
 * test. Finally we cover all code paths related to decrypting the private keys.
 */
public class KeyStoreIntegrationTest extends TestWithDeterministicJson {

  private Storage storage;
  private String projectId;
  private String bucket;
  private final String keyRingId = "test-ingestion-beam-integration";

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

  private void ensureKmsResources(KeyManagementServiceClient client, String resourceId) {
    CryptoKeyVersionName name = CryptoKeyVersionName.parse(resourceId);
    assertEquals(projectId, name.getProject());
    assertEquals("global", name.getLocation());
    assertEquals(keyRingId, name.getKeyRing());

    // getOrCreateKeyRing
    KeyRingName keyRingName = KeyRingName.of(projectId, "global", name.getKeyRing());
    try {
      client.getKeyRing(keyRingName);
    } catch (NotFoundException e) {
      LocationName parent = LocationName.of(projectId, "global");
      KeyRing request = KeyRing.newBuilder().build();
      client.createKeyRing(parent, name.getCryptoKey(), request);
    }

    // getOrCreateCryptoKey
    CryptoKeyName cryptoKeyName = CryptoKeyName.of(projectId, "global", name.getKeyRing(),
        name.getCryptoKey());
    try {
      client.getCryptoKey(cryptoKeyName);
    } catch (NotFoundException e) {
      CryptoKey request = CryptoKey.newBuilder()
          .setPurpose(CryptoKey.CryptoKeyPurpose.ASYMMETRIC_DECRYPT)
          .setVersionTemplate(CryptoKeyVersionTemplate.newBuilder()
              .setAlgorithm(CryptoKeyVersionAlgorithm.RSA_DECRYPT_OAEP_2048_SHA256))
          .build();
      client.createCryptoKey(keyRingName, name.getCryptoKey(), request);
    }
  }

  /**
   * Encrypt data using another library since Cloud KMS does not support
   * encryption using asymmetric keys.
   *
   * <p>https://stackoverflow.com/questions/17110217/is-rsa-pkcs1-oaep-padding-supported-in-bouncycastle
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

  /**
   * Ensure KMS permissions are configured as expected. This ensures a key ring
   * and crypto key exist. It then fetches the public key associated to the
   * crypto key and encodes a small string. Then the KMS api is called to
   * decrypt the message. This test (and following tests) require Cloud KMS
   * Admin, CryptoKey Decrypter, and Public Key Viewer.
   */
  @Test
  public void testKmsConfigured() throws IOException, InvalidCipherTextException {
    try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {
      String plainText = "hello world!";

      String cryptoKeyId = "testing-key";
      String resourceId = CryptoKeyVersionName.of(projectId, "global", keyRingId, cryptoKeyId, "1")
          .toString();
      ensureKmsResources(client, resourceId);

      byte[] cipherText = encrypt(client.getPublicKey(resourceId), plainText.getBytes("UTF-8"));
      String decrypted = client.asymmetricDecrypt(resourceId, ByteString.copyFrom(cipherText))
          .getPlaintext().toStringUtf8();

      assertEquals(plainText, decrypted);
    }
  }

}
