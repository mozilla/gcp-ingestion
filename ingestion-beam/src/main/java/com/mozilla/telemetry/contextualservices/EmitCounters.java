package com.mozilla.telemetry.contextualservices;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.metrics.PerDocTypeCounter;
import java.util.HashMap;
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
    return messages.apply(MapElements.into(TypeDescriptor.of(SponsoredInteraction.class))
        .via((SponsoredInteraction interaction) -> {
          // mock attributes based on the sponsored interaction fields
          Map<String, String> attributes = new HashMap<String, String>();
          String originalDocType = interaction.getOriginalDocType();
          if (originalDocType != null) {
            attributes.put(Attribute.DOCUMENT_TYPE, originalDocType);
          } else {
            attributes.put(Attribute.DOCUMENT_TYPE, interaction.getDerivedDocumentType());
          }

          if (interaction.getOriginalNamespace() != null) {
            attributes.put(Attribute.DOCUMENT_NAMESPACE, interaction.getOriginalNamespace());
          }

          PerDocTypeCounter.inc(attributes, "valid_submission");
          if (interaction.getRequestId() != null) {
            PerDocTypeCounter.inc(attributes, "valid_submission_merino");
          }
          return interaction;
        }));
  }

}
