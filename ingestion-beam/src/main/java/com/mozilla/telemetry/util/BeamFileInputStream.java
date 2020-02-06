package com.mozilla.telemetry.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import org.apache.beam.sdk.io.FileSystems;

public class BeamFileInputStream {

  /**
   * Open {@code path} using beam's {@link FileSystems}.
   */
  public static InputStream open(String path) throws IOException {
    return Channels
        .newInputStream(FileSystems.open(FileSystems.matchSingleFileSpec(path).resourceId()));
  }
}
