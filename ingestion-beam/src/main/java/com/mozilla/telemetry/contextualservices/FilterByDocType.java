package com.mozilla.telemetry.contextualservices;

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
  private final boolean limitLegacyDesktopVersion;

  private static transient Set<String> allowedDocTypesSet;
  private static transient Set<String> allowedNamespacesSet;

  @VisibleForTesting
  static synchronized void clearSingletonsForTests() {
    allowedDocTypesSet = null;
    allowedNamespacesSet = null;
  }

  /** Constructor. */
  public FilterByDocType(String allowedDocTypes, String allowedNamespaces,
      boolean limitLegacyDesktopVersion) {
    this.allowedDocTypes = allowedDocTypes;
    this.allowedNamespaces = allowedNamespaces;
    this.limitLegacyDesktopVersion = limitLegacyDesktopVersion;
  }

  public static FilterByDocType of(String allowedDocTypes, String allowedNamespaces,
      boolean limitLegacyDesktopVersion) {
    return new FilterByDocType(allowedDocTypes, allowedNamespaces, limitLegacyDesktopVersion);
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

      // Special handling for desktop pings.
      if ("contextual-services".equals(namespace) || "firefox-desktop".equals(namespace)) {
        // Verify Firefox version here so rejected messages don't go to error output
        final int minVersion;
        int maxVersion = Integer.MAX_VALUE;
        final boolean isLegacyDesktop;
        if (doctype.startsWith("topsites-")) {
          minVersion = 87;
          isLegacyDesktop = true;
        } else if (doctype.startsWith("quicksuggest-")) {
          minVersion = 89;
          isLegacyDesktop = true;
        } else if ("top-sites".equals(doctype)) {
          minVersion = 116;
          maxVersion = 136;
          isLegacyDesktop = false;
        } else if ("quick-suggest".equals(doctype)) {
          minVersion = 116;
          isLegacyDesktop = false;
        } else if ("search-with".equals(doctype)) {
          minVersion = 122;
          isLegacyDesktop = false;
        } else {
          PerDocTypeCounter.inc(message.getAttributeMap(), "doctype_filter_unhandled");
          return; // drop message
        }
        final String version = message.getAttribute(Attribute.USER_AGENT_VERSION);
        try {
          if (version == null || minVersion > Integer.parseInt(version)
              || maxVersion < Integer.parseInt(version) || (limitLegacyDesktopVersion
                  && isLegacyDesktop && 116 <= Integer.parseInt(version))) {
            PerDocTypeCounter.inc(message.getAttributeMap(), "version_filter_rejected");
            return; // drop message
          }
        } catch (NumberFormatException e) {
          PerDocTypeCounter.inc(message.getAttributeMap(), "version_filter_invalid");
          return; // drop message
        }
      }
      PerDocTypeCounter.inc(message.getAttributeMap(), "version_filter_passed");

      out.output(message);
    }
  }
}
