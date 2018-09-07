/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import com.mozilla.telemetry.transforms.CompositeTransform;
import com.mozilla.telemetry.transforms.DecodePubsubMessages;
import com.mozilla.telemetry.transforms.GeoCityLookup;
import com.mozilla.telemetry.transforms.GzipDecompress;
import com.mozilla.telemetry.transforms.ParseUri;
import com.mozilla.telemetry.transforms.ParseUserAgent;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;

public class Decoder extends Sink {
  public interface Options extends Sink.Options {
    @Description("Path to GeoIP2-City.mmdb")
    @Required
    String getGeoCityDatabase();

    void setGeoCityDatabase(String value);
  }

  /**
   * Execute an Apache Beam pipeline.
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    // register options class so that `--help=Options` works
    PipelineOptionsFactory.register(Options.class);

    final Options options = PipelineOptionsFactory
        .fromArgs(args)
        .withValidation()
        .as(Options.class);

    final Pipeline pipeline = Pipeline.create(options);
    final PTransform<PCollection<PubsubMessage>, PDone> errorOutput =
        options.getErrorOutputType().write(options);

    pipeline
        .apply("input", options.getInputType().read(options))
        .apply("write input parsing errors",
            CompositeTransform.of((PCollectionTuple input) -> {
              input.get(DecodePubsubMessages.errorTag).apply(errorOutput);
              return input.get(DecodePubsubMessages.mainTag);
            }))
        .apply("parseUri", new ParseUri())
        .apply("write parseUri errors",
            CompositeTransform.of((PCollectionTuple input) -> {
              input.get(ParseUri.errorTag).apply(errorOutput);
              return input.get(ParseUri.mainTag);
            }))
        .apply("decompress", new GzipDecompress())
        .apply("geoCityLookup", new GeoCityLookup(options.getGeoCityDatabase()))
        .apply("parseUserAgent", new ParseUserAgent())
        .apply("write main output", options.getOutputType().write(options));

    pipeline.run();
  }
}
