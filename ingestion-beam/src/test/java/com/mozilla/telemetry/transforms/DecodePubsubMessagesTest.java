package com.mozilla.telemetry.transforms;

import com.mozilla.telemetry.matchers.Lines;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DecodePubsubMessagesTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testText() {
    List<String> inputLines = Lines.resources("testdata/decode-pubsub-messages/input-*");

    PCollection<String> result = pipeline //
        .apply(Create.of(inputLines)) //
        .apply(InputFileFormat.text.decode()) //
        .apply(OutputFileFormat.text.encode());

    PAssert.that(result).containsInAnyOrder(inputLines);

    pipeline.run();
  }

  @Test
  public void testJson() {
    List<String> inputLines = Lines.resources("testdata/decode-pubsub-messages/input-valid*");
    List<String> validLines = Lines
        .resources("testdata/decode-pubsub-messages/output-normalized-json.ndjson");

    PCollection<String> result = pipeline //
        .apply(Create.of(inputLines)) //
        .apply(InputFileFormat.json.decode()) //
        .apply("EncodeJsonOutput", OutputFileFormat.json.encode());

    PAssert.that(result).containsInAnyOrder(validLines);

    pipeline.run();
  }

  @Test
  public void testJsonErrorThrows() {
    List<String> inputLines = Lines.resources("testdata/decode-pubsub-messages/input-*");
    pipeline.apply(Create.of(inputLines)).apply(InputFileFormat.json.decode());
    thrown.expectCause(Matchers.isA(UncheckedIOException.class));
    pipeline.run();
  }

}
