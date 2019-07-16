/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

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
