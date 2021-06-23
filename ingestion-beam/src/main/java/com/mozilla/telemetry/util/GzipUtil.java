package com.mozilla.telemetry.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import org.apache.commons.io.IOUtils;

public class GzipUtil {

  /**
   * If the input bytes are gzipped, return them decompressed; otherwise, return the input bytes
   * unmodified.
   */
  public static byte[] maybeDecompress(byte[] bytes) {
    try (ByteArrayInputStream payloadStream = new ByteArrayInputStream(bytes);
        GZIPInputStream gzipStream = new GZIPInputStream(payloadStream)) {
      return IOUtils.toByteArray(gzipStream);
    } catch (IOException e) {
      // This payload must not actually be gzip-encoded, so pass through the original bytes.
      return bytes;
    }
  }

  /**
   * Return true if the input is gzipped based on comparing the first two bytes to GZIP header.
   */
  public static boolean isGzip(byte[] bytes) {
    try (ByteArrayInputStream payloadStream = new ByteArrayInputStream(bytes)) {
      int header = payloadStream.read() | payloadStream.read() << 8;
      return header == GZIPInputStream.GZIP_MAGIC;
    } catch (IOException e) {
      return false;
    }
  }
}
