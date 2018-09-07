/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.transforms.DecodePubsubMessages;
import com.mozilla.telemetry.decoder.GeoCityLookup;
import com.mozilla.telemetry.decoder.GzipDecompress;
import com.mozilla.telemetry.decoder.ParseUri;
import com.mozilla.telemetry.decoder.ParseUserAgent;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class DecoderTest {
  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void gzipDecompress() {
    final List<String> input = Arrays.asList(
        "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}",
        // TODO calculate this compression for the test
        // payload="$(printf test | gzip -c | base64)"
        "{\"payload\":\"H4sIAM1ekFsAAytJLS4BAAx+f9gEAAAA\"}");

    final List<String> expected = Arrays.asList(
        "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":null,\"payload\":\"dGVzdA==\"}");

    final PCollection<String> output = pipeline
        .apply(Create.of(input))
        .apply("decodeJson", InputFileFormat.json.decode())
        .get(DecodePubsubMessages.mainTag)
        .apply("gzipDecompress", new GzipDecompress())
        .apply("encodeJson", OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }

  @Test
  public void geoCityLookup() {
    final List<String> input = Arrays.asList(
        "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":{\"remote_addr\":\"8.8.8.8\"},\"payload\":\"\"}",
        "{\"attributeMap\":"
            + "{\"remote_addr\":\"10.0.0.2\""
            + ",\"x_forwarded_for\":\"192.168.1.2, 63.245.208.195\""
            + "},\"payload\":\"\"}");

    final List<String> expected = Arrays.asList(
        "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":{\"geo_country\":\"US\"},\"payload\":\"\"}",
        "{\"attributeMap\":"
            + "{\"geo_country\":\"US\""
            + ",\"geo_city\":\"Sacramento\""
            + ",\"geo_subdivision1\":\"CA\""
            + "},\"payload\":\"\"}");

    final PCollection<String> output = pipeline
        .apply(Create.of(input))
        .apply("decodeJson", InputFileFormat.json.decode())
        .get(DecodePubsubMessages.mainTag)
        .apply("geoCityLookup", new GeoCityLookup("GeoLite2-City.mmdb"))
        .apply("encodeJson", OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }

  @Test
  public void parseUserAgent() {
    final List<String> input = Arrays.asList(
        "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":{\"user_agent\":\"\"},\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":"
            + "{\"user_agent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:63.0)"
            + " Gecko/20100101 Firefox/63.0\""
            + "},\"payload\":\"\"}");

    final List<String> expected = Arrays.asList(
        "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":{},\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":"
            + "{\"user_agent_browser\":\"Firefox\""
            + ",\"user_agent_version\":\"63.0\""
            + ",\"user_agent_os\":\"Macintosh\""
            + "},\"payload\":\"\"}");

    final PCollection<String> output = pipeline
        .apply(Create.of(input))
        .apply("decodeJson", InputFileFormat.json.decode())
        .get(DecodePubsubMessages.mainTag)
        .apply("parseUserAgent", new ParseUserAgent())
        .apply("encodeJson", OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }

  @Test
  public void parseUri() {
    final List<String> input = Arrays.asList(
        "{\"attributeMap\":"
            + "{\"uri\":\"/submit/telemetry/ce39b608-f595-4c69-b6a6-f7a436604648"
            + "/main/Firefox/61.0a1/nightly/20180328030202\""
            + "},\"payload\":\"\"}",
        "{\"attributeMap\":"
            + "{\"uri\":\"/submit/eng-workflow/hgpush/1/2c3a0767-d84a-4d02-8a92-fa54a3376049\""
            + "},\"payload\":\"\"}");

    final List<String> expected = Arrays.asList(
        "{\"attributeMap\":"
            + "{\"app_name\":\"Firefox\""
            + ",\"app_version\":\"61.0a1\""
            + ",\"app_build_id\":\"20180328030202\""
            + ",\"app_update_channel\":\"nightly\""
            + ",\"document_namespace\":\"telemetry\""
            + ",\"document_id\":\"ce39b608-f595-4c69-b6a6-f7a436604648\""
            + ",\"document_type\":\"main\""
            + "},\"payload\":\"\"}",
        "{\"attributeMap\":"
            + "{\"document_namespace\":\"eng-workflow\""
            + ",\"document_version\":\"1\""
            + ",\"document_id\":\"2c3a0767-d84a-4d02-8a92-fa54a3376049\""
            + ",\"document_type\":\"hgpush\""
            + "},\"payload\":\"\"}");

    final PCollection<String> output = pipeline
        .apply(Create.of(input))
        .apply("decodeJson", InputFileFormat.json.decode())
        .get(DecodePubsubMessages.mainTag)
        .apply("parseUserAgent", new ParseUri())
        .get(ParseUri.mainTag)
        .apply("encodeJson", OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }
}
