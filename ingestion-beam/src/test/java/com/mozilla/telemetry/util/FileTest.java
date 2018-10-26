/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.util;

import static org.junit.Assert.assertThat;

import java.io.FileNotFoundException;
import java.io.InputStream;
import org.hamcrest.Matchers;
import org.junit.Test;

public class FileTest {

  @Test
  public void inputStreamLocalTest() throws Exception {
    InputStream inputStream = File.inputStream("README.md");
    int size = 0;
    while (inputStream.read() != -1) {
      size++;
    }
    assertThat(size, Matchers.greaterThan(100)); // README.md is at least 100 bytes.
  }

  @Test(expected = FileNotFoundException.class)
  public void inputStreamThrowsOnMissingFile() throws Exception {
    File.inputStream("nonexistent.txt");
  }

  @Test(expected = FileNotFoundException.class)
  public void inputStreamThrowsOnDirectory() throws Exception {
    File.inputStream("bin");
  }

  @Test(expected = IllegalArgumentException.class)
  public void inputStreamThrowsOnMultipleMatches() throws Exception {
    File.inputStream("bin/*");
  }

}
