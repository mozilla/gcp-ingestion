/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import org.apache.commons.io.IOUtils;

public class GzipUtil {

  public static byte[] maybeDecompress(byte[] bytes) {
    try (ByteArrayInputStream payloadStream = new ByteArrayInputStream(bytes);
        GZIPInputStream gzipStream = new GZIPInputStream(payloadStream);
        ByteArrayOutputStream decompressedStream = new ByteArrayOutputStream();) {
      IOUtils.copy(gzipStream, decompressedStream);
      return decompressedStream.toByteArray();
    } catch (IOException e) {
      // This payload must not actually be gzip-encoded, so pass through the original bytes.
      return bytes;
    }
  }

}
