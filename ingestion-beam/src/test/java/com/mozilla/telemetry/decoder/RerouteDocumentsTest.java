package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class RerouteDocumentsTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testOutput() {
    final List<String> validInput = Arrays.asList(//
        "{\"attributeMap\":{\"document_namespace\":\"telemetry\"" //
            + ",\"document_type\": \"xfocsp-error-report\"},\"payload\":\"\"}",
        "{\"attributeMap\":{\"document_namespace\":\"telemetry\"" //
            + ",\"document_type\": \"main\"},\"payload\":\"\"}");

    final List<String> expected = Arrays.asList(//
        "{\"attributeMap\":{\"document_namespace\":\"xfocsp-error-report\"" //
            + ",\"document_type\":\"xfocsp-error-report\"},\"payload\":\"\"}",
        "{\"attributeMap\":{\"document_namespace\":\"telemetry\"" //
            + ",\"document_type\":\"main\"},\"payload\":\"\"}");

    PCollection<PubsubMessage> result = pipeline.apply(Create.of(validInput)) //
        .apply("DecodeJsonInput", InputFileFormat.json.decode()) //
        .apply("RerouteDocuments", RerouteDocuments.of());

    PCollection<String> output = result //
        .apply("EncodeJsonOutput", OutputFileFormat.json.encode());
    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }
}
