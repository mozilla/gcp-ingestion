package com.mozilla.telemetry.republisher;

import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.lang3.StringUtils;

public class RepublishPerChannel extends PTransform<PCollection<PubsubMessage>, PDone> {

  private RepublisherOptions baseOptions;

  public static RepublishPerChannel of(RepublisherOptions baseOptions) {
    return new RepublishPerChannel(baseOptions);
  }

  @Override
  public PDone expand(PCollection<PubsubMessage> input) {
    List<Destination> destinations = baseOptions.getPerChannelSampleRatios().entrySet().stream() //
        .map(Destination::new) //
        .collect(Collectors.toList());
    int numDestinations = destinations.size();
    int numPartitions = numDestinations + 1;
    PCollectionList<PubsubMessage> partitioned = input.apply("PartitionByChannel",
        Partition.of(numPartitions, new PartitionFn(destinations)));

    for (int i = 0; i < numDestinations; i++) {
      Destination destination = destinations.get(i);
      RepublisherOptions.Parsed opts = baseOptions.as(RepublisherOptions.Parsed.class);

      // The destination pattern here must be compile-time due to a detail of Dataflow's
      // streaming PubSub producer implementation; if that restriction is lifted in the future,
      // this can become a runtime parameter and we can perform replacement via NestedValueProvider.
      opts.setOutput(StaticValueProvider
          .of(baseOptions.getPerChannelDestination().replace("${channel}", destination.channel)));

      partitioned.get(i) //
          .apply("Sample" + destination.getCapitalizedChannel() + "BySampleIdOrRandomNumber",
              Filter.by(message -> {
                message = PubsubConstraints.ensureNonNull(message);
                String sampleId = message.getAttribute("sample_id");
                return RandomSampler.filterBySampleIdOrRandomNumber(sampleId, destination.ratio);
              }))
          .apply("Republish" + destination.getCapitalizedChannel() + "Sample",
              opts.getOutputType().write(opts));
    }

    return PDone.in(input.getPipeline());
  }

  private RepublishPerChannel(RepublisherOptions baseOptions) {
    this.baseOptions = baseOptions;
  }

  private static class PartitionFn implements Partition.PartitionFn<PubsubMessage> {

    private final List<Destination> destinations;

    @Override
    public int partitionFor(PubsubMessage message, int numPartitions) {
      message = PubsubConstraints.ensureNonNull(message);
      String channel = message.getAttribute("normalized_channel");
      for (int i = 0; i < destinations.size(); i++) {
        if (destinations.get(i).matches(channel)) {
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

    final String channel;
    final Double ratio;

    public Destination(Map.Entry<String, Double> entry) {
      this.channel = entry.getKey();
      this.ratio = entry.getValue();
    }

    public String getCapitalizedChannel() {
      return StringUtils.capitalize(channel);
    }

    public boolean matches(String channelToMatch) {
      return channel.equals(channelToMatch);
    }

  }
}
