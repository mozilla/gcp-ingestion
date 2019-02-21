/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import com.mozilla.telemetry.decoder.AddMetadata;
import com.mozilla.telemetry.decoder.DecoderOptions;
import com.mozilla.telemetry.decoder.Deduplicate;
import com.mozilla.telemetry.decoder.GeoCityLookup;
import com.mozilla.telemetry.decoder.ParsePayload;
import com.mozilla.telemetry.decoder.ParseUri;
import com.mozilla.telemetry.decoder.ParseUserAgent;
import com.mozilla.telemetry.transforms.DecompressPayload;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
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
    registerOptions(); // Defined in Sink.java
    final DecoderOptions.Parsed options = DecoderOptions.parseDecoderOptions(
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DecoderOptions.class));
    return run(options);
  }

  /**
   * Execute an Apache Beam pipeline and return the {@code PipelineResult}.
   */
  public static PipelineResult run(DecoderOptions.Parsed options) {
    final Pipeline pipeline = Pipeline.create(options);
    final List<PCollection<PubsubMessage>> errorCollections = new ArrayList<>();

    // Trailing comments are used below to prevent rewrapping by google-java-format.
    PCollection<PubsubMessage> deduplicated = pipeline //
        .apply(options.getInputType().read(options)).errorsTo(errorCollections) //
        .apply(ParseUri.of()).errorsTo(errorCollections) //
        .apply(DecompressPayload.enabled(options.getDecompressInputPayloads())) //
        .apply(ParsePayload.of(options.getSchemasLocation())).errorsTo(errorCollections) //
        .apply(GeoCityLookup.of(options.getGeoCityDatabase(), options.getGeoCityFilter())) //
        .apply(ParseUserAgent.of()) //
        .apply(AddMetadata.of()).errorsTo(errorCollections) //
        .apply(Deduplicate.removeDuplicates(options.getParsedRedisUri()))
        .sendDuplicateMetadataToErrors() //
        .errorsTo(errorCollections);

    // Write the main output collection.
    deduplicated.apply(options.getOutputType().write(options)).errorsTo(errorCollections);

    // Mark messages as seen in Redis.
    options
        .getSeenMessagesSource().read(options, deduplicated).apply(Deduplicate
            .markAsSeen(options.getParsedRedisUri(), options.getDeduplicateExpireSeconds()))
        .errorsTo(errorCollections);

    // Write error output collections.
    PCollectionList.of(errorCollections) //
        .apply("FlattenErrorCollections", Flatten.pCollections()) //
        .apply("WriteErrorOutput", options.getErrorOutputType().write(options)) //
        .output();

    return pipeline.run();
  }
}
