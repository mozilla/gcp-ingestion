/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import com.mozilla.telemetry.decoder.Deduplicate;
import com.mozilla.telemetry.republisher.RandomSampler;
import com.mozilla.telemetry.republisher.RepublisherOptions;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.commons.lang3.StringUtils;

public class Republisher extends Sink {

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
    final RepublisherOptions.Parsed options = RepublisherOptions.parseRepublisherOptions(
        PipelineOptionsFactory.fromArgs(args).withValidation().as(RepublisherOptions.class));
    return run(options);
  }

  /**
   * Execute an Apache Beam pipeline and return the {@code PipelineResult}.
   */
  public static PipelineResult run(RepublisherOptions.Parsed options) {
    // We aren't decoding payloads, so no need to recompress when republishing.
    options.setOutputPubsubCompression(StaticValueProvider.of(Compression.UNCOMPRESSED));

    final Pipeline pipeline = Pipeline.create(options);
    final List<PCollection<PubsubMessage>> errorCollections = new ArrayList<>();

    // Trailing comments are used below to prevent rewrapping by google-java-format.
    PCollection<PubsubMessage> decoded = pipeline //
        .apply(options.getInputType().read(options)).errorsTo(errorCollections);

    // Mark messages as seen in Redis.
    decoded //
        .apply(Deduplicate.markAsSeen(options.getParsedRedisUri(),
            options.getDeduplicateExpireSeconds()))
        .errorsTo(errorCollections);

    // Republish debug messages.
    if (options.getEnableDebugDestination()) {
      RepublisherOptions.Parsed opts = options.as(RepublisherOptions.Parsed.class);
      opts.setOutput(options.getDebugDestination());
      decoded //
          .apply("FilterDebugMessages", Filter.by(message -> {
            message = PubsubConstraints.ensureNonNull(message);
            return message.getAttribute("x_debug_id") != null;
          })) //
          .apply(opts.getOutputType().write(opts));
    }

    // Republish to per-docType destinations.
    final List<String> enabledDocTypes = Optional //
        .ofNullable(options.getPerDocTypeEnabledList()) //
        .orElse(Collections.emptyList());
    for (String entry : enabledDocTypes) {
      final String[] components = entry.split("/");
      final String targetNamespace;
      final String targetDocType;
      if (components.length == 1) {
        targetNamespace = "telemetry";
        targetDocType = components[0];
      } else {
        targetNamespace = components[0];
        targetDocType = components[1];
      }

      RepublisherOptions.Parsed opts = options.as(RepublisherOptions.Parsed.class);
      // The destination pattern here must be compile-time due to a detail of Dataflow's
      // streaming PubSub producer implementation; if that restriction is lifted in the future,
      // this can become a runtime parameter and we can perform replacement via NestedValueProvider.
      String destination = options.getPerDocTypeDestination()
          .replace("${document_namespace}", targetNamespace)
          .replace("${document_type}", targetDocType);
      opts.setOutput(StaticValueProvider.of(destination));

      decoded //
          .apply("DocType is " + targetDocType, Filter.by(message -> {
            message = PubsubConstraints.ensureNonNull(message);
            return targetNamespace.equals(message.getAttribute("document_namespace"))
                && targetDocType.equals(message.getAttribute("document_type"));
          })) //
          .apply(opts.getOutputType().write(opts));
    }

    // Republish to sampled per-channel destinations.
    final Map<String, Double> ratioMap = Optional //
        .ofNullable(options.getPerChannelSampleRatios()) //
        .orElse(new HashMap<>());
    for (Map.Entry<String, Double> entry : ratioMap.entrySet()) {
      String targetChannel = entry.getKey();
      Double ratio = entry.getValue();

      RepublisherOptions.Parsed opts = options.as(RepublisherOptions.Parsed.class);
      // The destination pattern here must be compile-time due to a detail of Dataflow's
      // streaming PubSub producer implementation; if that restriction is lifted in the future,
      // this can become a runtime parameter and we can perform replacement via NestedValueProvider.
      String destination = options.getPerChannelDestination().replace("${channel}", targetChannel);
      opts.setOutput(StaticValueProvider.of(destination));

      decoded //
          .apply("RandomlySample" + StringUtils.capitalize(targetChannel), Filter.by(message -> {
            message = PubsubConstraints.ensureNonNull(message);
            String channel = message.getAttribute("app_update_channel");
            String sampleId = message.getAttribute("sample_id");
            return targetChannel.equals(channel)
                && RandomSampler.filterBySampleIdOrRandomNumber(sampleId, ratio);
          })) //
          .apply(opts.getOutputType().write(opts));
    }

    // Write error output collections.
    PCollectionList.of(errorCollections) //
        .apply("FlattenErrorCollections", Flatten.pCollections()) //
        .apply("WriteErrorOutput", options.getErrorOutputType().write(options)) //
        .output();

    return pipeline.run();
  }
}
