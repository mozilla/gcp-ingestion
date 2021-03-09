package com.mozilla.telemetry.contextualservices;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.metrics.PerDocTypeCounter;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class FilterByDocType
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  private final ValueProvider<List<String>> allowedDocTypes;

  public FilterByDocType(ValueProvider<List<String>> allowedDocTypes) {
    this.allowedDocTypes = allowedDocTypes;
  }

  public static FilterByDocType of(ValueProvider<List<String>> allowedDocTypes) {
    return new FilterByDocType(allowedDocTypes);
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(ParDo.of(new Fn()));
  }

  private class Fn extends DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage message, OutputReceiver<PubsubMessage> out) {
      message = PubsubConstraints.ensureNonNull(message);
      if (allowedDocTypes.isAccessible() && (allowedDocTypes.get() == null
          || allowedDocTypes.get().contains(message.getAttribute(Attribute.DOCUMENT_TYPE)))) {
        out.output(message);
        PerDocTypeCounter.inc(message.getAttributeMap(), "doctype_filter_passed");
      } else {
        PerDocTypeCounter.inc(message.getAttributeMap(), "doctype_filter_rejected");
      }
    }
  }
}
