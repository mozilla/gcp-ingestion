/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import static org.junit.Assert.assertEquals;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.transforms.WithErrors;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

public class AddMetadataTest extends TestWithDeterministicJson {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testOutput() {
    final List<String> input = Arrays.asList("{}", "{\"id\":null}", "[]", "{");
    Map<String, String> attributes = ImmutableMap.<String, String>builder().put("sample_id", "18")
        .put("geo_country", "CA").put("x_debug_id", "mysession")
        .put("normalized_channel", "release").build();
    WithErrors.Result<PCollection<PubsubMessage>> output = pipeline //
        .apply(Create.of(input)) //
        .apply("DecodeTextInput", InputFileFormat.text.decode()).output() //
        .apply("AddAttributes",
            MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                .via(element -> new PubsubMessage(element.getPayload(), attributes)))
        .apply(AddMetadata.of());

    final List<String> expectedMain = ImmutableList.of(//
        "{\"metadata\":{\"geo\":{\"country\":\"CA\"}" //
            + ",\"header\":{\"x_debug_id\":\"mysession\"}" //
            + ",\"user_agent\":{}}" //
            + ",\"normalized_channel\":\"release\"" //
            + ",\"sample_id\":18}", //
        "{\"id\":null," //
            + "\"metadata\":{\"geo\":{\"country\":\"CA\"}" //
            + ",\"header\":{\"x_debug_id\":\"mysession\"}" //
            + ",\"user_agent\":{}}" //
            + ",\"normalized_channel\":\"release\"" //
            + ",\"sample_id\":18}");
    final List<String> expectedError = Arrays.asList("{", "[]");
    final PCollection<String> error = output.errors() //
        .apply("EncodeTextError", OutputFileFormat.text.encode());
    PAssert.that(error).containsInAnyOrder(expectedError);

    final PCollection<String> main = output.output() //
        .apply("EncodeTextMain", MapElements.into(TypeDescriptors.strings())
            .via(m -> sortJson(OutputFileFormat.text.encodeSingleMessage(m))));
    PAssert.that(main).containsInAnyOrder(expectedMain);

    pipeline.run();
  }

  @Test
  public void testGeoFromAttributes() {
    Map<String, String> attributes = ImmutableMap.of("geo_country", "CA", //
        "sample_id", "15", "geo_city", "Whistler");
    Map<String, Object> geo = AddMetadata.geoFromAttributes(attributes);
    Map<String, Object> expected = ImmutableMap.of("country", "CA", "city", "Whistler");
    assertEquals(expected, geo);
  }

  @Test
  public void testPutGeoAttributes() {
    Map<String, Object> metadata = ImmutableMap.of("geo",
        ImmutableMap.of("country", "CA", "city", "Whistler"));
    Map<String, String> attributes = new HashMap<>();
    AddMetadata.putGeoAttributes(attributes, Json.asObjectNode(metadata));
    Map<String, String> expected = ImmutableMap.of("geo_country", "CA", "geo_city", "Whistler");
    assertEquals(expected, attributes);
  }

  @Test
  public void testUserAgentFromAttributes() {
    Map<String, String> attributes = ImmutableMap.of("user_agent_browser", "Firefox", //
        "user_agent_version", "63.0", //
        "sample_id", "3", //
        "user_agent_os", "Macintosh");
    Map<String, Object> expected = ImmutableMap.of("browser", "Firefox", //
        "version", "63.0", //
        "os", "Macintosh");
    Map<String, Object> userAgent = AddMetadata.userAgentFromAttributes(attributes);
    assertEquals(expected, userAgent);
  }

  @Test
  public void testPutUserAgentAttributes() {
    Map<String, Object> metadata = ImmutableMap.of("user_agent",
        ImmutableMap.of("browser", "Firefox", //
            "version", "63.0", //
            "os", "Macintosh"));
    Map<String, String> expected = ImmutableMap.of("user_agent_browser", "Firefox", //
        "user_agent_version", "63.0", //
        "user_agent_os", "Macintosh");
    Map<String, String> attributes = new HashMap<>();
    AddMetadata.putUserAgentAttributes(attributes, Json.asObjectNode(metadata));
    assertEquals(expected, attributes);
  }

  @Test
  public void testHeadersFromAttributes() {
    Map<String, String> attributes = ImmutableMap.of("dnt", "1", //
        "sample_id", "18", //
        "x_debug_id", "mysession");
    Map<String, Object> headers = AddMetadata.headersFromAttributes(attributes);
    Map<String, Object> expected = ImmutableMap.of("dnt", "1", "x_debug_id", "mysession");
    assertEquals(expected, headers);
  }

  @Test
  public void testPutHeaderAttributes() {
    Map<String, Object> metadata = ImmutableMap.of("header",
        ImmutableMap.of("dnt", "1", "x_debug_id", "mysession"));
    Map<String, String> expected = ImmutableMap.of("dnt", "1", //
        "x_debug_id", "mysession");
    Map<String, String> attributes = new HashMap<>();
    AddMetadata.putHeaderAttributes(attributes, Json.asObjectNode(metadata));
    assertEquals(expected, attributes);
  }

  @Test
  public void testUriFromAttributes() {
    Map<String, String> attributes = ImmutableMap //
        .of("uri", "/submit/eng-workflow/hgpush/1/2c3a0767-d84a-4d02-8a92-fa54a3376049", //
            "app_name", "Firefox", //
            "sample_id", "18", //
            "app_update_channel", "release");
    Map<String, Object> uri = AddMetadata.uriFromAttributes(attributes);
    Map<String, Object> expected = ImmutableMap //
        .of("uri", "/submit/eng-workflow/hgpush/1/2c3a0767-d84a-4d02-8a92-fa54a3376049", //
            "app_name", "Firefox", "app_update_channel", "release");
    assertEquals(expected, uri);
  }

  @Test
  public void testPutUriAttributes() {
    Map<String, Object> metadata = ImmutableMap.of("uri", ImmutableMap //
        .of("app_name", "Firefox", "app_update_channel", "release"));
    Map<String, String> expected = ImmutableMap.of("app_name", "Firefox", //
        "app_update_channel", "release");
    Map<String, String> attributes = new HashMap<>();
    AddMetadata.putUriAttributes(attributes, Json.asObjectNode(metadata));
    assertEquals(expected, attributes);
  }

  @Test
  public void testAttributesToMetadataPayload() {
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put("document_namespace", "telemetry") //
        .put("app_name", "Firefox") //
        .put("sample_id", "18") //
        .put("geo_country", "CA") //
        .put("x_debug_id", "mysession") //
        .put("normalized_channel", "release") //
        .put("x_forwarded_for", "??") //
        .build();
    Map<String, Object> payload = AddMetadata.attributesToMetadataPayload(attributes);
    Map<String, Object> expected = ImmutableMap.<String, Object>builder() //
        .put("metadata", ImmutableMap.<String, Object>builder() //
            .put("document_namespace", "telemetry") //
            .put("uri", ImmutableMap.of("app_name", "Firefox")) //
            .put("header", ImmutableMap.of("x_debug_id", "mysession")) //
            .put("geo", ImmutableMap.of("country", "CA")) //
            .put("user_agent", ImmutableMap.of()) //
            .build()) //
        .put("normalized_channel", "release") //
        .put("sample_id", 18) //
        .build();
    assertEquals(expected, payload);
  }

  @Test
  public void testStripPayloadMetadataToAttributes() {
    Map<String, Object> metadata = ImmutableMap.<String, Object>builder() //
        .put("uri", ImmutableMap.of("app_name", "Firefox"))
        .put("header", ImmutableMap.of("x_debug_id", "mysession"))
        .put("geo", ImmutableMap.of("country", "CA")).put("user_agent", ImmutableMap.of()).build();
    ObjectNode payload = Json.createObjectNode();
    payload.set("metadata", Json.asObjectNode(metadata));
    payload.put("field1", 99);
    payload.put("normalized_channel", "release");
    payload.put("sample_id", 18);
    Map<String, String> expected = ImmutableMap.<String, String>builder().put("app_name", "Firefox")
        .put("sample_id", "18").put("geo_country", "CA").put("x_debug_id", "mysession")
        .put("normalized_channel", "release").build();
    Map<String, String> attributes = new HashMap<>();
    AddMetadata.stripPayloadMetadataToAttributes(attributes, payload);
    assertEquals(expected, attributes);
    assertEquals(ImmutableMap.of("field1", 99), Json.asMap(payload));
  }

}
