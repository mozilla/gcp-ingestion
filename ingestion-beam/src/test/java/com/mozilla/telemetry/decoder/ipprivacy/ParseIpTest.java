package com.mozilla.telemetry.decoder.ipprivacy;

import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class ParseIpTest extends TestWithDeterministicJson {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testParseIp() {
    final List<String> input = Collections
        .singletonList("{\"attributeMap\":" + "{\"remote_addr\":\"127.0.0.1\""
            + ",\"x_forwarded_for\":\"123.321.123,45.54.45,67.76.67\"" + "},\"payload\":\"\"}");

    PCollection<String> output = pipeline.apply(Create.of(input))
        .apply(InputFileFormat.json.decode()).apply(ParseIp.of())
        .apply(OutputFileFormat.json.encode());

    final List<String> expected = Collections.singletonList(
        "{\"attributeMap\":" + "{\"client_ip\":\"45.54.45\"" + ",\"remote_addr\":\"127.0.0.1\""
            + ",\"x_forwarded_for\":\"123.321.123,45.54.45,67.76.67\"" + "},\"payload\":\"\"}");

    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }
}
