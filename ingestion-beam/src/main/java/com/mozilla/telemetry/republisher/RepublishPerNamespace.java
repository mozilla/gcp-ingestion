/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.republisher;

import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;

public class RepublishPerNamespace extends PTransform<PCollection<PubsubMessage>, PDone> {

  private RepublisherOptions baseOptions;

  public static RepublishPerNamespace of(RepublisherOptions baseOptions) {
    return new RepublishPerNamespace(baseOptions);
  }

  @Override
  public PDone expand(PCollection<PubsubMessage> input) {
    List<Destination> destinations = baseOptions.getPerNamespaceEnabledList().stream()//
        .map(Destination::new) //
        .collect(Collectors.toList());
    int numDestinations = destinations.size();
    int numPartitions = numDestinations + 1;
    PCollectionList<PubsubMessage> partitioned = input.apply("PartitionByNamespace",
        Partition.of(numPartitions, new PartitionFn(destinations)));

    for (int i = 0; i < numDestinations; i++) {
      Destination destination = destinations.get(i);
      RepublisherOptions.Parsed opts = baseOptions.as(RepublisherOptions.Parsed.class);

      // The destination pattern here must be compile-time due to a detail of Dataflow's
      // streaming PubSub producer implementation; if that restriction is lifted in the future,
      // this can become a runtime parameter and we can perform replacement via NestedValueProvider.
      opts.setOutput(StaticValueProvider.of(baseOptions.getPerNamespaceDestination()
          .replace("${document_namespace}", destination.namespace)));

      String name = String.join("_", "republish", destination.namespace);
      partitioned.get(i).apply(name, opts.getOutputType().write(opts));
    }

    return PDone.in(input.getPipeline());
  }

  private RepublishPerNamespace(RepublisherOptions baseOptions) {
    this.baseOptions = baseOptions;
  }

  private static class PartitionFn implements Partition.PartitionFn<PubsubMessage> {

    private final List<Destination> destinations;

    @Override
    public int partitionFor(PubsubMessage message, int numPartitions) {
      message = PubsubConstraints.ensureNonNull(message);
      String namespace = message.getAttribute("document_namespace");
      for (int i = 0; i < destinations.size(); i++) {
        if (destinations.get(i).matches(namespace)) {
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

    final String namespace;

    public Destination(String namespace) {
      this.namespace = namespace;
    }

    public boolean matches(String namespaceToMatch) {
      return namespace.equals(namespaceToMatch);
    }

  }
}
