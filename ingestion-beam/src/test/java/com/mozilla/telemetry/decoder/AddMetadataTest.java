package com.mozilla.telemetry.decoder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;

public class AddMetadataTest extends TestWithDeterministicJson {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testOutput() {
    final List<String> input = Arrays.asList("{}", "{\"id\":null}", "[]", "{");
    Map<String, String> attributes = ImmutableMap.<String, String>builder().put("sample_id", "18")
        .put("geo_country", "CA").put("isp_name", "service provider").put("x_debug_id", "mysession")
        .put("normalized_channel", "release").build();
    WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage> output = pipeline //
        .apply(Create.of(input)) //
        .apply("DecodeTextInput", InputFileFormat.text.decode()) //
        .apply("AddAttributes",
            MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                .via(element -> new PubsubMessage(element.getPayload(), attributes)))
        .apply(AddMetadata.of());

    final List<String> expectedMain = ImmutableList.of(//
        "{\"metadata\":{\"geo\":{\"country\":\"CA\"}" //
            + ",\"isp\":{\"name\":\"service provider\"}" //
            + ",\"user_agent\":{}" //
            + ",\"header\":{\"x_debug_id\":\"mysession\"}}" //
            + ",\"normalized_channel\":\"release\"" //
            + ",\"sample_id\":18}", //
        "{\"metadata\":{\"geo\":{\"country\":\"CA\"}" //
            + ",\"isp\":{\"name\":\"service provider\"}" //
            + ",\"user_agent\":{}" //
            + ",\"header\":{\"x_debug_id\":\"mysession\"}}" //
            + ",\"normalized_channel\":\"release\"" //
            + ",\"sample_id\":18" //
            + ",\"id\":null}");
    final List<String> expectedError = Arrays.asList("{", "[]");
    final PCollection<String> error = output.failures() //
        .apply("EncodeTextError", OutputFileFormat.text.encode());
    PAssert.that(error).containsInAnyOrder(expectedError);

    final PCollection<String> main = output.output() //
        .apply("EncodeTextMain", OutputFileFormat.text.encode());
    PAssert.that(main).containsInAnyOrder(expectedMain);

    pipeline.run();
  }
}
