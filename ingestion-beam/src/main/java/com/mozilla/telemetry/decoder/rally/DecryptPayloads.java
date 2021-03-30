package com.mozilla.telemetry.decoder.rally;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class DecryptPayloads extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  private final ValueProvider<String> metadataLocation;
  private final ValueProvider<String> schemasLocation;
  private final ValueProvider<Boolean> kmsEnabled;
  private final ValueProvider<Boolean> decompressPayload;

  public static DecryptPayloads of(ValueProvider<String> metadataLocation,
      ValueProvider<String> schemasLocation, ValueProvider<Boolean> kmsEnabled,
      ValueProvider<Boolean> decompressPayload) {
    return new DecryptPayloads(metadataLocation, schemasLocation, kmsEnabled, decompressPayload);
  }

  private DecryptPayloads(ValueProvider<String> metadataLocation,
      ValueProvider<String> schemasLocation, ValueProvider<Boolean> kmsEnabled,
      ValueProvider<Boolean> decompressPayload) {
    this.metadataLocation = metadataLocation;
    this.schemasLocation = schemasLocation;
    this.kmsEnabled = kmsEnabled;
    this.decompressPayload = decompressPayload;
  }

  @Override
  public Result<PCollection<PubsubMessage>, PubsubMessage> expand(
      PCollection<PubsubMessage> messages) {

    final List<PCollection<PubsubMessage>> failureCollections = new ArrayList<>();

    PCollectionList<PubsubMessage> partitioned = messages //
        .apply("PartitionRallyAndPioneer", Partition.of(2,
            (PubsubMessage message, int numPartitions) -> isPioneerPing(message) ? 1 : 0));

    PCollection<PubsubMessage> rally = partitioned.get(0)
        .apply("DecryptRallyPayloads",
            DecryptRallyPayloads.of(metadataLocation, schemasLocation, kmsEnabled,
                decompressPayload)) //
        .failuresTo(failureCollections);

    PCollection<PubsubMessage> pioneer = partitioned.get(1)
        .apply("DecryptPioneerPayloads",
            DecryptPioneerPayloads.of(metadataLocation, kmsEnabled, decompressPayload)) //
        .failuresTo(failureCollections);

    PCollection<PubsubMessage> output = PCollectionList.of(rally).and(pioneer) //
        .apply("FlattenDecryptedRallyMessages", Flatten.pCollections());
    PCollection<PubsubMessage> errors = PCollectionList.of(failureCollections) //
        .apply("FlattenRallyErrors", Flatten.pCollections());
    return WithFailures.Result.of(output, errors);
  }

  private static Boolean isPioneerPing(PubsubMessage message) {
    message = PubsubConstraints.ensureNonNull(message);
    Map<String, String> attributes = new HashMap<>(message.getAttributeMap());
    final String namespace = attributes.get(Attribute.DOCUMENT_NAMESPACE);
    final String docType = attributes.get(Attribute.DOCUMENT_TYPE);
    return "telemetry".equals(namespace) && "pioneer-study".equals(docType);
  }
}
