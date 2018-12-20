/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import com.mozilla.telemetry.decoder.AddMetadata;
import com.mozilla.telemetry.decoder.DecoderOptions;
import com.mozilla.telemetry.decoder.Deduplicate;
import com.mozilla.telemetry.decoder.GeoCityLookup;
import com.mozilla.telemetry.decoder.GzipDecompress;
import com.mozilla.telemetry.decoder.ParsePayload;
import com.mozilla.telemetry.decoder.ParseUri;
import com.mozilla.telemetry.decoder.ParseUserAgent;
import com.mozilla.telemetry.transforms.ParseSubmissionTimestamp;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class Decoder extends Sink {

  /**
   * Execute an Apache Beam pipeline.
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    run(args);
  }

  /**
   * Execute an Apache Beam pipeline and return the {@code PipelineResult}.
   *
   * @param args command line arguments
   */
  public static PipelineResult run(String[] args) {
    final DecoderOptions.Parsed options = DecoderOptions.parseDecoderOptions(
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DecoderOptions.class));
    // register options class so that `--help=DecoderOptions` works
    PipelineOptionsFactory.register(DecoderOptions.class);

    return run(options);
  }

  /**
   * Execute an Apache Beam pipeline and return the {@code PipelineResult}.
   */
  public static PipelineResult run(DecoderOptions.Parsed options) {
    final Pipeline pipeline = Pipeline.create(options);
    final List<PCollection<PubsubMessage>> errorCollections = new ArrayList<>();

    // Trailing comments are used below to prevent rewrapping by google-java-format.
    pipeline //
        .apply("input", options.getInputType().read(options)) //
        .addErrorCollectionTo(errorCollections).output() //
        .apply("parseUri", new ParseUri()) //
        .addErrorCollectionTo(errorCollections).output() //
        .apply("decompress", new GzipDecompress()) //
        .apply("parsePayload", new ParsePayload()) //
        .addErrorCollectionTo(errorCollections).output() //
        .apply("geoCityLookup",
            new GeoCityLookup(options.getGeoCityDatabase(), options.getGeoCityFilter()))
        .apply("parseUserAgent", new ParseUserAgent()) //
        .apply("addMetadata", new AddMetadata()) //
        .addErrorCollectionTo(errorCollections).output() //
        .apply("removeDuplicates", Deduplicate.removeDuplicates(options.getParsedRedisUri()))
        .addErrorCollectionTo(errorCollections).output() //
        .apply("markAsSeen", PTransform.compose((PCollection<PubsubMessage> input) -> {
          options
              .getSeenMessagesSource().read(options, input).apply(Deduplicate
                  .markAsSeen(options.getParsedRedisUri(), options.getDeduplicateExpireSeconds()))
              .addErrorCollectionTo(errorCollections);
          return input;
        })) //
        .apply(ParseSubmissionTimestamp.enabled(options.getParseSubmissionTimestamp())) //
        .apply("write main output", options.getOutputType().write(options))
        .addErrorCollectionTo(errorCollections).output();

    PCollectionList.of(errorCollections).apply(Flatten.pCollections()).apply("write error output",
        options.getErrorOutputType().write(options));

    return pipeline.run();
  }
}
