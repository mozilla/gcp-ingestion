package com.mozilla.telemetry.republisher;

import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;

public class RepublishPerDocType extends PTransform<PCollection<PubsubMessage>, PDone> {

  private RepublisherOptions baseOptions;

  public static RepublishPerDocType of(RepublisherOptions baseOptions) {
    return new RepublishPerDocType(baseOptions);
  }

  @Override
  public PDone expand(PCollection<PubsubMessage> input) {
    List<Destination> destinations = baseOptions.getPerDocTypeDestinations().entrySet().stream()
        .flatMap(
            entry -> entry.getValue().stream().map(value -> new Destination(entry.getKey(), value)))
        .collect(Collectors.toList());

    int numDestinations = destinations.size();
    int numPartitions = numDestinations + 1;
    PCollectionList<PubsubMessage> partitioned = input.apply("PartitionByDocType",
        Partition.of(numPartitions, new PartitionFn(destinations)));

    for (int i = 0; i < numDestinations; i++) {
      Destination destination = destinations.get(i);
      RepublisherOptions.Parsed opts = baseOptions.as(RepublisherOptions.Parsed.class);
      opts.setOutput(destination.dest);
      String name = String.join("_", "republish", destination.namespace, destination.docType);
      partitioned.get(i).apply(name, opts.getOutputType().write(opts));
    }

    return PDone.in(input.getPipeline());
  }

  private RepublishPerDocType(RepublisherOptions baseOptions) {
    this.baseOptions = baseOptions;
  }

  private static class PartitionFn implements Partition.PartitionFn<PubsubMessage> {

    private final List<Destination> destinations;

    @Override
    public int partitionFor(PubsubMessage message, int numPartitions) {
      message = PubsubConstraints.ensureNonNull(message);
      String docType = message.getAttribute("document_type");
      String namespace = message.getAttribute("document_namespace");
      for (int i = 0; i < destinations.size(); i++) {
        if (destinations.get(i).matches(namespace, docType)) {
          return i;
        }
      }
      // The last partition catches docTypes that aren't configured to have a destination;
      // these will be ignored.
      return numPartitions - 1;
    }

    public PartitionFn(List<Destination> destinations) {
      this.destinations = destinations;
    }
  }

  private static class Destination implements Serializable {

    final String docType;
    final String dest;
    final String namespace;

    public Destination(String dest, String entry) {
      this.dest = dest;
      final String[] components = entry.split("/");
      if (components.length != 2) {
        throw new IllegalArgumentException("Each entry of perDocTypeEnabledList must be in the"
            + " form namespace/doctype, but found " + entry);
      }
      this.namespace = components[0];
      this.docType = components[1];
    }

    public boolean matches(String namespaceToMatch, String docTypeToMatch) {
      return namespace.equals(namespaceToMatch) && docType.equals(docTypeToMatch);
    }
  }
}
