package com.mozilla.telemetry.amplitude;

import com.google.common.annotations.VisibleForTesting;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.metrics.PerDocTypeCounter;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;

public class FilterByDocType
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  private final String allowedDocTypes;
  private final String allowedNamespaces;

  private static transient Set<String> allowedDocTypesSet;
  private static transient Set<String> allowedNamespacesSet;

  @VisibleForTesting
  static synchronized void clearSingletonsForTests() {
    allowedDocTypesSet = null;
    allowedNamespacesSet = null;
  }

  /** Constructor. */
  public FilterByDocType(String allowedDocTypes, String allowedNamespaces) {
    this.allowedDocTypes = allowedDocTypes;
    this.allowedNamespaces = allowedNamespaces;
  }

  public static FilterByDocType of(String allowedDocTypes, String allowedNamespaces) {
    return new FilterByDocType(allowedDocTypes, allowedNamespaces);
  }

  private Set<String> parseAllowlistString(String allowlistString, String argument) {
    if (allowlistString == null) {
      throw new IllegalArgumentException(
          String.format("Required --%s argument not found", argument));
    }
    return Arrays.stream(allowlistString.split(",")).filter(StringUtils::isNotBlank)
        .collect(Collectors.toSet());
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(ParDo.of(new Fn()));
  }

  private class Fn extends DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage message, OutputReceiver<PubsubMessage> out) {
      message = PubsubConstraints.ensureNonNull(message);
      if (allowedNamespacesSet == null || allowedDocTypesSet == null) {
        allowedDocTypesSet = parseAllowlistString(allowedDocTypes, "allowedDocTypes");
        allowedNamespacesSet = parseAllowlistString(allowedNamespaces, "allowedNamespaces");
      }

      final String namespace = message.getAttribute(Attribute.DOCUMENT_NAMESPACE);
      final String doctype = message.getAttribute(Attribute.DOCUMENT_TYPE);

      if (!allowedNamespacesSet.contains(namespace) || !allowedDocTypesSet.contains(doctype)) {
        PerDocTypeCounter.inc(message.getAttributeMap(), "doctype_filter_rejected");
        return;
      }
      PerDocTypeCounter.inc(message.getAttributeMap(), "doctype_filter_passed");

      out.output(message);
    }
  }
}
