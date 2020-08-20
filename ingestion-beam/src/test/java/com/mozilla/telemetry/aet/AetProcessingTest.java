package com.mozilla.telemetry.aet;

import static com.mozilla.telemetry.aet.DecryptAetIdentifiersTest.encryptWithTestPublicKey;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.mozilla.telemetry.decoder.DecoderOptions;
import com.mozilla.telemetry.decoder.DecoderOptions.Parsed;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;

public class AetProcessingTest extends TestWithDeterministicJson {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testOutputTelemetry() throws Exception {
    // minimal test for throughput of a single document
    DecoderOptions decoderOptions = pipeline.getOptions().as(DecoderOptions.class);
    decoderOptions.setAetEnabled(true);
    decoderOptions.setAetMetadataLocation(pipeline
        .newProvider(Resources.getResource("account-ecosystem/metadata-local.json").getPath()));
    decoderOptions.setAetKmsEnabled(pipeline.newProvider(false));
    Parsed options = DecoderOptions.parseDecoderOptions(decoderOptions);
    String userId = "2ef67665ec7369ba0fab7f802a5e1e04";
    String anonId = encryptWithTestPublicKey(userId);
    List<String> prevUserIds = ImmutableList.of("3ef67665ec7369ba0fab7f802a5e1e04",
        "4ef67665ec7369ba0fab7f802a5e1e04");
    List<String> prevAnonIds = prevUserIds.stream().map(x -> {
      try {
        return encryptWithTestPublicKey(x);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
    String input = Json.asString(ImmutableMap.of("payload",
        ImmutableMap.builder().put("ecosystemAnonId", anonId)
            .put("ecosystemClientId", "3ed15efab7e94757bf9e9ef5e844ada2")
            .put("ecosystemDeviceId", "7ab4e373ce434b848a9d0946e388fee9")
            .put("previousEcosystemAnonIds", prevAnonIds).build()));
    String expected = Json.asString(ImmutableMap.of("payload",
        ImmutableMap.builder().put("ecosystemUserId", userId)
            .put("ecosystemClientId", "3ed15efab7e94757bf9e9ef5e844ada2")
            .put("ecosystemDeviceId", "7ab4e373ce434b848a9d0946e388fee9")
            .put("previousEcosystemUserIds", prevUserIds).build()));
    Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline.apply(Create.of(input))
        .apply(InputFileFormat.text.decode())
        .apply("AddAttributes",
            MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                .via(element -> new PubsubMessage(element.getPayload(),
                    ImmutableMap.of(Attribute.SUBMISSION_TIMESTAMP, "2020-01-01 12:00:00",
                        Attribute.URI,
                        "/submit/telemetry/ce39b608-f595-4c69-b6a6-f7a436604648"
                            + "/account-ecosystem/Firefox/61.0a1/nightly/20180328030202"))))
        .apply(AetProcessing.of(options));
    PAssert.that(result.failures()).empty();
    PAssert.that(result.output().apply(OutputFileFormat.text.encode()))
        .containsInAnyOrder(ImmutableList.of(expected));
    pipeline.run();
  }

}
