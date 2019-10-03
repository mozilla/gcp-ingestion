package com.mozilla.telemetry.util;

import org.apache.beam.sdk.io.FileIO;

public class NoColonFileNaming {

  public static FileIO.Write.FileNaming defaultNaming(String prefix, String suffix) {
    return of(FileIO.Write.defaultNaming(prefix, suffix));
  }

  public static FileIO.Write.FileNaming of(FileIO.Write.FileNaming naming) {
    return (window, pane, numShards, shardIndex, compression) -> naming
        .getFilename(window, pane, numShards, shardIndex, compression).replaceAll(":", "-");
  }
}
