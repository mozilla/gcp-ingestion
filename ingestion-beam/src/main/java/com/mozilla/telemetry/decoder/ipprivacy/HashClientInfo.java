package com.mozilla.telemetry.decoder.ipprivacy;

import com.google.common.annotations.VisibleForTesting;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class HashClientInfo
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  private final ValueProvider<String> clientIdHashKeyPath;
  private final ValueProvider<String> clientIpHashKeyPath;

  private static byte[] clientIdHashKey;
  private static byte[] clientIpHashKey;

  public static HashClientInfo of(ValueProvider<String> clientIdHashKeyPath,
      ValueProvider<String> clientIpHashKeyPath) {
    return new HashClientInfo(clientIdHashKeyPath, clientIpHashKeyPath);
  }

  private HashClientInfo(ValueProvider<String> clientIdHashKeyPath,
      ValueProvider<String> clientIpHashKeyPath) {
    this.clientIdHashKeyPath = clientIdHashKeyPath;
    this.clientIpHashKeyPath = clientIpHashKeyPath;
  }

  @VisibleForTesting
  static class KeyLengthMismatchException extends Exception {

    KeyLengthMismatchException(int keyLength) {
      super("Key length was " + keyLength + ", expected 32");
    }
  }

  @VisibleForTesting
  static class MissingKeyException extends Exception {

    MissingKeyException() {
      super("Path to hash keys for client ip and client id must be provided to the pipeline");
    }
  }

  @VisibleForTesting
  static class IdenticalKeyException extends Exception {

    IdenticalKeyException() {
      super("ip and id hash keys must not be identical");
    }
  }

  private String keyedHash(String input, byte[] key)
      throws InvalidKeyException, NoSuchAlgorithmException {
    final String algorithmName = "HmacSHA256";
    Mac hmac = Mac.getInstance(algorithmName);
    SecretKeySpec secretKeySpec = new SecretKeySpec(key, algorithmName);

    hmac.init(secretKeySpec);

    byte[] hashed = hmac.doFinal(input.getBytes(StandardCharsets.UTF_8));

    StringBuilder sb = new StringBuilder();
    for (byte b : hashed) {
      sb.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
    }
    return sb.toString();
  }

  @VisibleForTesting
  byte[] getClientIdHashKey() throws IOException, KeyLengthMismatchException {
    if (clientIdHashKey == null) {
      clientIdHashKey = readBytes(clientIdHashKeyPath.get());
    }
    return clientIdHashKey;
  }

  @VisibleForTesting
  byte[] getClientIpHashKey() throws IOException, KeyLengthMismatchException {
    if (clientIpHashKey == null) {
      clientIpHashKey = readBytes(clientIpHashKeyPath.get());
    }
    return clientIpHashKey;
  }

  @VisibleForTesting
  byte[] readBytes(String uri) throws IOException, KeyLengthMismatchException {
    Metadata metadata = FileSystems.matchSingleFileSpec(uri);
    ReadableByteChannel inputChannel = FileSystems.open(metadata.resourceId());
    try (InputStream inputStream = Channels.newInputStream(inputChannel)) {
      byte[] key = new byte[32];
      int bytesRead = inputStream.read(key);
      if (bytesRead != 32) {
        throw new KeyLengthMismatchException(bytesRead);
      }
      return key;
    }
  }

  @VisibleForTesting
  static boolean isHashed(String input) {
    return input != null && input.length() == 64 && input.matches("^[a-zA-Z0-9]+$");
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(
        MapElements.into(TypeDescriptor.of(PubsubMessage.class)).via((PubsubMessage message) -> {
          Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

          if (!clientIdHashKeyPath.isAccessible() || clientIdHashKeyPath.get() == null
              || !clientIpHashKeyPath.isAccessible() || clientIpHashKeyPath.get() == null) {
            throw new RuntimeException(new MissingKeyException());
          }

          String clientId = attributes.get(Attribute.CLIENT_ID);
          String clientIp = attributes.get(Attribute.CLIENT_IP);

          byte[] clientIdKey;
          byte[] clientIpKey;

          try {
            clientIdKey = getClientIdHashKey();
            clientIpKey = getClientIpHashKey();
          } catch (KeyLengthMismatchException | IOException e) {
            throw new RuntimeException(e);
          }

          if (Arrays.equals(clientIdKey, clientIpKey)) {
            throw new RuntimeException(new IdenticalKeyException());
          }
          try {
            if (clientId != null && !isHashed(clientId)) {
              String hashedClientId = keyedHash(clientId, clientIdKey);
              attributes.put(Attribute.CLIENT_ID, hashedClientId);
            }
            if (clientIp != null && !isHashed(clientIp)) {
              String hashedClientIp = keyedHash(clientIp, clientIpKey);
              attributes.put(Attribute.CLIENT_IP, hashedClientIp);
            }
          } catch (InvalidKeyException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
          }

          return new PubsubMessage(message.getPayload(), attributes);
        }));
  }
}
