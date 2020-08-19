package com.mozilla.telemetry.aet;

import com.mozilla.telemetry.decoder.DecoderOptions;
import com.mozilla.telemetry.decoder.ParseLogEntry;
import com.mozilla.telemetry.decoder.ParseUri;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

/**
 * Composite transform that handles decrypting AET identifiers and surrounding logic.
 *
 * <p>In particular, this transform provides special error handling such that payloads
 * are sanitized of identifiers before being included in the error collection to avoid
 * spilling sensitive ecosystem_anon_id values to error output.
 */
public class AetProcessing extends PTransform<PCollection<PubsubMessage>, //
    WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage>> {

  public static AetProcessing of(DecoderOptions options) {
    return new AetProcessing(options);
  }

  private AetProcessing(DecoderOptions options) {
    this.options = options;
  }

  private final DecoderOptions options;

  @Override
  public Result<PCollection<PubsubMessage>, PubsubMessage> expand(
      PCollection<PubsubMessage> input) {
    final List<PCollection<PubsubMessage>> failureCollections = new ArrayList<>();

    PCollection<PubsubMessage> output = input //
        .apply(ParseLogEntry.of()).failuresTo(failureCollections) //
        // ParseUri also appears in the Decoder after AET decryption,
        // but we have a redundant step here so that if there's a failure, we're able to
        // sanitize the payload before sending to error output.
        .apply("AetParseUri", ParseUri.of()).failuresTo(failureCollections) //
        .apply(DecryptAetIdentifiers //
            .of(options.getAetMetadataLocation(), options.getAetKmsEnabled())) //
        .failuresTo(failureCollections);

    // Flatten and sanitize error collections.
    PCollection<PubsubMessage> errors = PCollectionList.of(failureCollections) //
        .apply("FlattenAetFailureCollections", Flatten.pCollections()) //
        .apply("SanitizeJsonPayload", SanitizeJsonPayload.of());

    return WithFailures.Result.of(output, errors);
  }

}
