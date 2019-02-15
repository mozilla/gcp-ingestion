/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import static com.mozilla.telemetry.matchers.Lines.matchesInAnyOrder;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.mozilla.telemetry.matchers.Lines;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.options.OutputType;
import com.mozilla.telemetry.options.SinkOptions;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import nl.basjes.shaded.org.springframework.core.io.Resource;
import nl.basjes.shaded.org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.TimestampedValue;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests of our windowing and triggering semantics for file-based output. We want to be sure that
 * we don't lose any of the incoming messages, even in streaming mode with pathologically delayed
 * or out-of-order messages.
 */
public class FileWindowingTest implements Serializable {

  private Instant baseTime = new Instant(0);

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule
  public TemporaryFolder outputFolder = new TemporaryFolder();

  private String outputPath;

  @Before
  public void initialize() {
    outputPath = outputFolder.getRoot().getAbsolutePath();
  }

  @Test
  public void testDroppedEvents() {
    String output = outputPath + "/out";

    SinkOptions options = PipelineOptionsFactory.create().as(SinkOptions.class);
    options.setOutputType(OutputType.file);
    options.setOutputFileFormat(OutputFileFormat.text);
    options.setOutputFileCompression(Compression.UNCOMPRESSED);
    options.setOutputNumShards(pipeline.newProvider(1));
    options.setWindowDuration("10 minutes");
    options.setOutput(pipeline.newProvider(output));

    SinkOptions.Parsed parsedOptions = SinkOptions.parseSinkOptions(options);

    TestStream<PubsubMessage> messages = TestStream.create(PubsubMessageWithAttributesCoder.of())
        // Start at the epoch.
        .advanceWatermarkTo(baseTime)
        // Add some elements ahead of the watermark; these should all hit one file.
        .addElements(message("m1: before watermark", Duration.standardSeconds(3)),
            message("m2: before watermark", Duration.standardSeconds(1)),
            message("m3: before watermark", Duration.standardSeconds(22)),
            message("m4: before watermark", Duration.standardSeconds(3)))
        // Advance watermark past first window.
        .advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(11)))
        // Add some on-time elements (which should end up in a file for a new window) and some
        // late elements (which should end up in a file for a new pane of the first window).
        .addElements(message("m5: after watermark", Duration.standardMinutes(12)),
            message("m6: late arrival", Duration.standardSeconds(34)),
            message("m7: late arrival", Duration.standardSeconds(52)),
            message("m8: late arrival", Duration.standardSeconds(3)))
        // Test that we still capture messages near the 7 day Pub/Sub expiration;
        // this very late message should create a new pane and thus a new file.
        .advanceWatermarkTo(baseTime.plus(Duration.standardDays(6)))
        .addElements(message("m9: late arrival", Duration.standardMinutes(3)))
        // Finalize the stream.
        .advanceWatermarkToInfinity();

    pipeline.apply(messages).apply(OutputType.file.write(parsedOptions));
    pipeline.run().waitUntilFinish();

    List<String> expectedFileNames = ImmutableList.of(
        "out-1970-01-01T00:00:00.000Z-1970-01-01T00:10:00.000Z-0-00000-of-00001.txt",
        "out-1970-01-01T00:00:00.000Z-1970-01-01T00:10:00.000Z-1-00000-of-00001.txt",
        "out-1970-01-01T00:00:00.000Z-1970-01-01T00:10:00.000Z-2-00000-of-00001.txt",
        "out-1970-01-01T00:10:00.000Z-1970-01-01T00:20:00.000Z-0-00000-of-00001.txt");

    assertThat(Lines.files(outputPath + "/out*.txt"), Matchers.hasSize(9));
    assertThat(fileNames(outputPath + "/out*.txt"), matchesInAnyOrder(expectedFileNames));
  }

  private TimestampedValue<PubsubMessage> message(String content, Duration baseTimeOffset) {
    return TimestampedValue.of(new PubsubMessage(content.getBytes(), new HashMap<>()),
        baseTime.plus(baseTimeOffset));
  }

  private List<String> fileNames(String glob) {
    try {
      List<String> names = new ArrayList<>();
      Resource[] resources = new PathMatchingResourcePatternResolver().getResources("file:" + glob);
      for (Resource resource : resources) {
        names.add(resource.getFilename());
      }
      return names;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
