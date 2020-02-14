package com.mozilla.telemetry;

import com.mozilla.telemetry.decoder.DecoderOptions;
import com.mozilla.telemetry.decoder.Deduplicate;
import com.mozilla.telemetry.decoder.GeoCityLookup;
import com.mozilla.telemetry.decoder.ParseProxy;
import com.mozilla.telemetry.decoder.ParseUri;
import com.mozilla.telemetry.decoder.ipprivacy.HashClientInfo;
import com.mozilla.telemetry.decoder.ipprivacy.IpParsePayload;
import com.mozilla.telemetry.decoder.ipprivacy.ParseIp;
import com.mozilla.telemetry.decoder.ipprivacy.RemoveAttributes;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.DecompressPayload;
import com.mozilla.telemetry.transforms.DeduplicateByDocumentId;
import com.mozilla.telemetry.transforms.LimitPayloadSize;
import com.mozilla.telemetry.transforms.NormalizeAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

/**
 * Job for generating the IP privacy dataset that is based on the decoder job with
 * unneeded steps taken out.
 */
public class IpPrivacyDecoder extends Sink {

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

    // We wrap pipeline in Optional for more convenience in chaining together transforms.
    Optional.of(pipeline) //
        .map(p -> p //
            .apply(options.getInputType().read(options)) //
            .apply(ParseUri.of()).failuresTo(errorCollections) //
            .apply("RestrictToMainPings", Filter.by((message) -> {
              String doctype = message.getAttribute(Attribute.DOCUMENT_TYPE);
              return doctype != null && doctype.equals("main");
            })).apply(ParseProxy.of()) //
            .apply(ParseIp.of()) //
            .apply(GeoCityLookup.of(options.getGeoCityDatabase(), options.getGeoCityFilter())) //
            .apply(DecompressPayload.enabled(options.getDecompressInputPayloads())) //
            // See discussion in https://github.com/mozilla/gcp-ingestion/issues/776
            .apply(LimitPayloadSize.toMB(8)).failuresTo(errorCollections) //
            .apply(IpParsePayload.of()).failuresTo(errorCollections) //
            .apply(HashClientInfo.of(options.getClientIdHashKey(), options.getClientIpHashKey())) //
            .apply(NormalizeAttributes.of()) //
            .apply(Deduplicate.removeDuplicates(options.getParsedRedisUri()))
            .sendDuplicateMetadataToErrors().errorsTo(errorCollections)) //
        .map(p -> options.getDeduplicateByDocumentId() ? p.apply(DeduplicateByDocumentId.of()) : p)
        .map(p -> p //
            .apply(RemoveAttributes.of()) //
            .apply(options.getOutputType().write(options)).errorsTo(errorCollections));

    // Write error output collections.
    PCollectionList.of(errorCollections) //
        .apply("FlattenErrorCollections", Flatten.pCollections()) //
        .apply("WriteErrorOutput", options.getErrorOutputType().write(options)) //
        .output();

    return pipeline.run();
  }
}
