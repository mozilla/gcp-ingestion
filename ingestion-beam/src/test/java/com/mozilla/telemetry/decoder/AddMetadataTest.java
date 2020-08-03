package com.mozilla.telemetry.decoder;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
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

  private ObjectNode mapToObjectNode(Map<String, Object> m) throws IOException {
    return Json.readObjectNode(Json.asBytes(m));
  }

  @Test
  public void testGeoFromAttributes() throws Exception {
    Map<String, String> attributes = ImmutableMap.of("geo_country", "CA", //
        "sample_id", "15", "geo_city", "Whistler");
    ObjectNode geo = AddMetadata.geoFromAttributes(attributes);
    ObjectNode expected = mapToObjectNode(ImmutableMap.of("country", "CA", "city", "Whistler"));
    assertEquals(expected, geo);
  }

  @Test
  public void testPutGeoAttributes() throws Exception {
    ObjectNode metadata = mapToObjectNode(
        ImmutableMap.of("geo", ImmutableMap.of("country", "CA", "city", "Whistler")));
    Map<String, String> attributes = new HashMap<>();
    AddMetadata.putGeoAttributes(attributes, metadata);
    Map<String, String> expected = ImmutableMap.of("geo_country", "CA", "geo_city", "Whistler");
    assertEquals(expected, attributes);
  }

  @Test
  public void testUserAgentFromAttributes() throws Exception {
    Map<String, String> attributes = ImmutableMap.of("user_agent_browser", "Firefox", //
        "user_agent_version", "63.0", //
        "sample_id", "3", //
        "user_agent_os", "Macintosh");
    ObjectNode expected = mapToObjectNode(ImmutableMap.of("browser", "Firefox", //
        "version", "63.0", //
        "os", "Macintosh"));
    ObjectNode userAgent = AddMetadata.userAgentFromAttributes(attributes);
    assertEquals(expected, userAgent);
  }

  @Test
  public void testPutUserAgentAttributes() throws Exception {
    ObjectNode metadata = mapToObjectNode(ImmutableMap.of("user_agent",
        ImmutableMap.of("browser", "Firefox", //
            "version", "63.0", //
            "os", "Macintosh")));
    Map<String, String> expected = ImmutableMap.of("user_agent_browser", "Firefox", //
        "user_agent_version", "63.0", //
        "user_agent_os", "Macintosh");
    Map<String, String> attributes = new HashMap<>();
    AddMetadata.putUserAgentAttributes(attributes, metadata);
    assertEquals(expected, attributes);
  }

  @Test
  public void testHeadersFromAttributes() throws Exception {
    Map<String, String> attributes = ImmutableMap.of("dnt", "1", //
        "sample_id", "18", //
        "x_source_tags", "automation, perf", //
        "x_debug_id", "mysession");
    ObjectNode headers = AddMetadata.headersFromAttributes(attributes);
    ObjectNode expected = mapToObjectNode(ImmutableMap.of("dnt", "1", "x_debug_id", "mysession",
        "x_source_tags", "automation, perf"));
    assertEquals(expected, headers);
  }

  @Test
  public void testPutHeaderAttributes() throws Exception {
    ObjectNode metadata = mapToObjectNode(ImmutableMap.of("header", ImmutableMap.of("dnt", "1",
        "x_debug_id", "mysession", "x_source_tags", "automation, perf")));
    Map<String, String> expected = ImmutableMap.of("dnt", "1", //
        "x_debug_id", "mysession", "x_source_tags", "automation, perf");
    Map<String, String> attributes = new HashMap<>();
    AddMetadata.putHeaderAttributes(attributes, (metadata));
    assertEquals(expected, attributes);
  }

  @Test
  public void testUriFromAttributes() throws Exception {
    Map<String, String> attributes = ImmutableMap //
        .of("uri", "/submit/eng-workflow/hgpush/1/2c3a0767-d84a-4d02-8a92-fa54a3376049", //
            "app_name", "Firefox", //
            "sample_id", "18", //
            "app_update_channel", "release");
    ObjectNode uri = AddMetadata.uriFromAttributes(attributes);
    ObjectNode expected = mapToObjectNode(ImmutableMap //
        .of("uri", "/submit/eng-workflow/hgpush/1/2c3a0767-d84a-4d02-8a92-fa54a3376049", //
            "app_name", "Firefox", "app_update_channel", "release"));
    assertEquals(expected, uri);
  }

  @Test
  public void testPutUriAttributes() throws Exception {
    ObjectNode metadata = mapToObjectNode(ImmutableMap.of("uri", ImmutableMap //
        .of("app_name", "Firefox", "app_update_channel", "release")));
    Map<String, String> expected = ImmutableMap.of("app_name", "Firefox", //
        "app_update_channel", "release");
    Map<String, String> attributes = new HashMap<>();
    AddMetadata.putUriAttributes(attributes, (metadata));
    assertEquals(expected, attributes);
  }

  @Test
  public void testAttributesToMetadataPayload() throws Exception {
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put("document_namespace", "telemetry") //
        .put("app_name", "Firefox") //
        .put("sample_id", "18") //
        .put("geo_country", "CA") //
        .put("isp_name", "my isp") //
        .put("x_debug_id", "mysession") //
        .put("normalized_channel", "release") //
        .put("x_forwarded_for", "??") //
        .build();
    ObjectNode payload = AddMetadata.attributesToMetadataPayload(attributes);
    ObjectNode expected = mapToObjectNode(ImmutableMap.<String, Object>builder() //
        .put("metadata", ImmutableMap.<String, Object>builder() //
            .put("document_namespace", "telemetry") //
            .put("uri", ImmutableMap.of("app_name", "Firefox")) //
            .put("header", ImmutableMap.of("x_debug_id", "mysession")) //
            .put("geo", ImmutableMap.of("country", "CA")) //
            .put("isp", ImmutableMap.of("name", "my isp")) //
            .put("user_agent", ImmutableMap.of()) //
            .build()) //
        .put("normalized_channel", "release") //
        .put("sample_id", 18) //
        .build());
    assertEquals(expected, payload);
  }

  @Test
  public void testStripPayloadMetadataToAttributes() throws Exception {
    ObjectNode metadata = mapToObjectNode(ImmutableMap.<String, Object>builder() //
        .put("uri", ImmutableMap.of("app_name", "Firefox"))
        .put("header", ImmutableMap.of("x_debug_id", "mysession"))
        .put("geo", ImmutableMap.of("country", "CA")).put("user_agent", ImmutableMap.of()).build());
    ObjectNode payload = Json.createObjectNode();
    payload.set("metadata", metadata);
    payload.put("field1", 99);
    payload.put("normalized_channel", "release");
    payload.put("sample_id", 18);
    Map<String, String> expected = ImmutableMap.<String, String>builder().put("app_name", "Firefox")
        .put("sample_id", "18").put("geo_country", "CA").put("x_debug_id", "mysession")
        .put("normalized_channel", "release").build();
    Map<String, String> attributes = new HashMap<>();
    AddMetadata.stripPayloadMetadataToAttributes(attributes, payload);
    assertEquals(expected, attributes);
    assertEquals(mapToObjectNode(ImmutableMap.of("field1", 99)), payload);
  }

  private String reformatJson(byte[] data) throws Exception {
    return Json.asString(Json.readObjectNode(data));
  }

  private String reformatJson(String data) throws Exception {
    return reformatJson(data.getBytes(Charsets.UTF_8));
  }

  @Test
  public void testMergedPayload() throws Exception {
    String expect = reformatJson("{\"test\":\"foo\"}");

    byte[] payload = "{}".getBytes(Charsets.UTF_8);
    ObjectNode node = Json.createObjectNode();
    node.put("test", "foo");
    byte[] actual = AddMetadata.mergedPayload(payload, Json.asBytes(node));

    assertEquals(expect, reformatJson(actual));
  }

  @Test(expected = UncheckedIOException.class)
  public void testMergedPayloadInvalidPayload() throws Exception {
    byte[] payload = " {}".getBytes(Charsets.UTF_8);
    ObjectNode node = Json.createObjectNode();
    AddMetadata.mergedPayload(payload, Json.asBytes(node));
  }

}
