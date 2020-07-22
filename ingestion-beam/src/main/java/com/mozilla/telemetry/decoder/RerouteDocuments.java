package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A temporary shim that allows us to isolate a few document types to their own
 * datasets in BigQuery by rewriting the document_namespace attribute.
 *
 * <p>See https://bugzilla.mozilla.org/show_bug.cgi?id=1654558
 */
public class RerouteDocuments {

  private static final String XFOCSP_ERROR_REPORT = "xfocsp-error-report";

  /**
   * Return a transform that rewrites document_namespace for certain document types.
   */
  public static PTransform<PCollection<? extends PubsubMessage>, PCollection<PubsubMessage>> of() {
    return MapElements.into(TypeDescriptor.of(PubsubMessage.class)).via((PubsubMessage m) -> {
      if (ParseUri.TELEMETRY.equals(m.getAttribute(Attribute.DOCUMENT_NAMESPACE))
          && XFOCSP_ERROR_REPORT.equals(m.getAttribute(Attribute.DOCUMENT_TYPE))) {
        Map<String, String> attributes = new HashMap<>(m.getAttributeMap());
        attributes.put(Attribute.DOCUMENT_NAMESPACE, XFOCSP_ERROR_REPORT);
        return new PubsubMessage(m.getPayload(), attributes);
      } else {
        return m;
      }
    });
  }

}
