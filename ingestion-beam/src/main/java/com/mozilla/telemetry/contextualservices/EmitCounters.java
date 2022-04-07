package com.mozilla.telemetry.contextualservices;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.metrics.PerDocTypeCounter;

import java.util.Map;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Populate some counter metrics; payload passes through unchanged.
 */
public class EmitCounters
    extends PTransform<PCollection<SponsoredInteraction>, PCollection<SponsoredInteraction>> {

  public static EmitCounters of() {
    return new EmitCounters();
  }

  private EmitCounters() {
  }

  @Override
  public PCollection<SponsoredInteraction> expand(PCollection<SponsoredInteraction> messages) {
    return messages.apply(MapElements
            .into(TypeDescriptor.of(SponsoredInteraction.class))
            .via((SponsoredInteraction interaction) -> {
              // mock a doctype based on the sponsered interaction fields
              Map<String, String> attributes = Map.of(Attribute.DOCUMENT_TYPE, interaction.getDocumentType());

              PerDocTypeCounter.inc(attributes, "valid_submission");
              if (interaction.getRequestId() != null) {
                PerDocTypeCounter.inc(attributes, "valid_submission_merino");
              }
              return interaction;
            }));
  }

}
