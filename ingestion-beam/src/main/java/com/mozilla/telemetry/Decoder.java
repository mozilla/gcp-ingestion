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
import com.mozilla.telemetry.transforms.DecodePubsubMessages;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;

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
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DecoderOptions.class));
    final Pipeline pipeline = Pipeline.create(options);
    final List<PCollection<PubsubMessage>> errorCollections = new ArrayList<>();

    pipeline.apply("input", options.getInputType().read(options))
        .apply("collect input parsing errors", PTransform.compose((PCollectionTuple input) -> {
          errorCollections.add(input.get(DecodePubsubMessages.errorTag));
          return input.get(DecodePubsubMessages.mainTag);
        })).apply("parseUri", new ParseUri())
        .apply("collect parseUri errors", PTransform.compose((PCollectionTuple input) -> {
          errorCollections.add(input.get(ParseUri.errorTag));
          return input.get(ParseUri.mainTag);
        })).apply("validateSchema", new ValidateSchema())
        .apply("collect validateSchema errors", PTransform.compose((PCollectionTuple input) -> {
          errorCollections.add(input.get(ValidateSchema.errorTag));
          return input.get(ValidateSchema.mainTag);
        })).apply("decompress", new GzipDecompress())
        .apply("geoCityLookup",
            new GeoCityLookup(options.getGeoCityDatabase(), options.getGeoCityFilter()))
        .apply("parseUserAgent", new ParseUserAgent()).apply("addMetadata", new AddMetadata())
        .apply("collect addMetadata errors", PTransform.compose((PCollectionTuple input) -> {
          errorCollections.add(input.get(AddMetadata.errorTag));
          return input.get(AddMetadata.mainTag);
        })).apply("removeDuplicates", Deduplicate.removeDuplicates(options.getRedisUri()))
        .apply("collect removeDuplicates errors", PTransform.compose((PCollectionTuple input) -> {
          errorCollections.add(input.get(Deduplicate.errorTag));
          return input.get(Deduplicate.mainTag);
        })).apply("markAsSeen", PTransform.compose((PCollection<PubsubMessage> input) -> {
          errorCollections.add(options
              .getSeenMessagesSource().read(options, input).apply(Deduplicate
                  .markAsSeen(options.getRedisUri(), options.getDeduplicateExpireSeconds()))
              .get(Deduplicate.errorTag));
          return input;
        })).apply("write main output", options.getOutputType().write(options))
        .apply("collect output errors", PTransform.compose((PCollection<PubsubMessage> input) -> {
          errorCollections.add(input);
          return input;
        }));

    PCollectionList.of(errorCollections).apply(Flatten.pCollections()).apply("write error output",
        options.getErrorOutputType().write(options));

    pipeline.run();
  }
}
