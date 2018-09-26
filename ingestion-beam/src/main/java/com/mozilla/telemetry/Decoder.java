/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import com.mozilla.telemetry.decoder.AddMetadata;
import com.mozilla.telemetry.decoder.DecoderOptions;
import com.mozilla.telemetry.decoder.Deduplicate;
import com.mozilla.telemetry.decoder.GeoCityLookup;
import com.mozilla.telemetry.decoder.GzipDecompress;
import com.mozilla.telemetry.decoder.ParseUri;
import com.mozilla.telemetry.decoder.ParseUserAgent;
import com.mozilla.telemetry.decoder.ValidateSchema;
import com.mozilla.telemetry.transforms.CompositeTransform;
import com.mozilla.telemetry.transforms.DecodePubsubMessages;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.POutput;

public class Decoder extends Sink {
  /**
   * Execute an Apache Beam pipeline.
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    // register options class so that `--help=DecoderOptions` works
    PipelineOptionsFactory.register(DecoderOptions.class);

    final DecoderOptions.Parsed options = DecoderOptions.parseDecoderOptions(
        PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(DecoderOptions.class)
    );

    final Pipeline pipeline = Pipeline.create(options);
    final PTransform<PCollection<PubsubMessage>, ? extends POutput> errorOutput =
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
        .apply("validateSchema", new ValidateSchema())
        .apply("write validateSchema errors",
            CompositeTransform.of((PCollectionTuple input) -> {
              input.get(ValidateSchema.errorTag).apply(errorOutput);
              return input.get(ValidateSchema.mainTag);
            }))
        .apply("decompress", new GzipDecompress())
        .apply("geoCityLookup", new GeoCityLookup(options.getGeoCityDatabase()))
        .apply("parseUserAgent", new ParseUserAgent())
        .apply("addMetadata", new AddMetadata())
        .apply("write addMetadata errors",
            CompositeTransform.of((PCollectionTuple input) -> {
              input.get(AddMetadata.errorTag).apply(errorOutput);
              return input.get(AddMetadata.mainTag);
            }))
        .apply("removeDuplicates", Deduplicate.removeDuplicates(options.getRedisUri()))
        .apply("write removeDuplicates errors",
            CompositeTransform.of((PCollectionTuple input) -> {
              input.get(Deduplicate.errorTag).apply(errorOutput);
              return input.get(Deduplicate.mainTag);
            }))
        .apply("markAsSeen",
            CompositeTransform.of((PCollection<PubsubMessage> input) -> {
              options.getSeenMessagesSource()
                  .read(options, input)
                  .apply(Deduplicate
                      .markAsSeen(
                          options.getRedisUri(), options.getDeduplicateExpireSeconds()))
                  .get(Deduplicate.errorTag)
                  .apply(errorOutput);
              return input;
            }))
        .apply("write main output", options.getOutputType().write(options))
        .apply("write output errors", errorOutput);

    pipeline.run();
  }
}
