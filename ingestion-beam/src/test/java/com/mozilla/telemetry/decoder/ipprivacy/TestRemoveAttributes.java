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

public class TestRemoveAttributes extends TestWithDeterministicJson {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testRemoveAttributes() {
    final List<String> input = Collections.singletonList("{\"attributeMap\":" //
        + "{\"submission_timestamp\":\"2000-01-01T00:00:00.000000Z\""
        + ",\"remote_addr\":\"127.0.0.1\"" + ",\"x_forwarded_for\":\"1,2,3\""
        + ",\"x_forwarded_for\":\"1,2,3\"" + ",\"geo_country\":\"US\""
        + ",\"normalized_country_code\":\"US\"" + ",\"client_id\":\"abc\"" + ",\"client_ip\":\"2\""
        + ",\"app_name\":\"app_name\"" + ",\"os\":\"os\"" + "},\"payload\":\"\"}");

    PCollection<String> output = pipeline.apply(Create.of(input))
        .apply(InputFileFormat.json.decode()).apply(RemoveAttributes.of())
        .apply(OutputFileFormat.json.encode());

    final List<String> expected = Collections.singletonList("{\"attributeMap\":"
        + "{\"client_id\":\"abc\"" + ",\"client_ip\":\"2\"" + ",\"geo_country\":\"US\""
        + ",\"submission_timestamp\":\"2000-01-01T00:00:00.000000Z\"" + "},\"payload\":\"\"}");

    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }

}
