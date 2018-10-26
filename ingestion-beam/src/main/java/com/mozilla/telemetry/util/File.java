/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;

public class File {

  /**
   * Fetch a single local or remote file using Beam's {@link FileSystems} machinery.
   *
   * @param spec a path on the local file system or in GCS (gs://bucket/path)
   * @return the file contents as {@link InputStream}
   */
  public static InputStream inputStream(String spec) throws IOException {
    MatchResult match = FileSystems.match(spec);
    if (match.metadata().isEmpty()) {
      throw new IllegalArgumentException("No file found matching " + spec);
    }
    if (match.metadata().size() > 1) {
      throw new IllegalArgumentException(
          "Multiple files found matching " + spec + " when only one was expected");

    }

    ResourceId resourceId = match.metadata().get(0).resourceId();
    ReadableByteChannel channel = FileSystems.open(resourceId);
    return Channels.newInputStream(channel);
  }
}
