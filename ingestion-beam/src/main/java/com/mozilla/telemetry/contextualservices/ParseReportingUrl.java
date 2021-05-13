package com.mozilla.telemetry.contextualservices;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.metrics.PerDocTypeCounter;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.BeamFileInputStream;
import com.mozilla.telemetry.util.Json;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithFailures.ExceptionElement;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Extract reporting URL from document and filter out unknown URLs.
 */
public class ParseReportingUrl extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  private final ValueProvider<String> urlAllowList;

  private static transient Set<String> singletonAllowedImpressionUrls;
  private static transient Set<String> singletonAllowedClickUrls;

  // Values from the user_agent_os attribute
  private static final String OS_WINDOWS = "Windows";
  private static final String OS_MAC = "Macintosh";
  private static final String OS_LINUX = "Linux";

  // Values used for the os-family API parameter
  private static final String PARAM_WINDOWS = "Windows";
  private static final String PARAM_MAC = "macOS";
  private static final String PARAM_LINUX = "Linux";

  public static ParseReportingUrl of(ValueProvider<String> urlAllowList) {
    return new ParseReportingUrl(urlAllowList);
  }

  private ParseReportingUrl(ValueProvider<String> urlAllowList) {
    this.urlAllowList = urlAllowList;
  }

  @Override
  public Result<PCollection<PubsubMessage>, PubsubMessage> expand(
      PCollection<PubsubMessage> messages) {
    return messages.apply(
        MapElements.into(TypeDescriptor.of(PubsubMessage.class)).via((PubsubMessage message) -> {
          message = PubsubConstraints.ensureNonNull(message);

          ObjectNode payload;
          try {
            payload = Json.readObjectNode(message.getPayload());
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }

          try {
            loadAllowedUrls();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }

          Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

          String reportingUrl = payload.path(Attribute.REPORTING_URL).asText();

          // TODO: change back to reportingUrl
          ReportingUrlUtil urlParser = new ReportingUrlUtil("https://test.com?id=abc");

          if (!isUrlValid(urlParser.getReportingUrl(), attributes.get(Attribute.DOCUMENT_TYPE))) {
            PerDocTypeCounter.inc(attributes, "RejectedNonNullUrl");
            throw new ReportingUrlUtil.InvalidUrlException(
                // TODO: change back to reportingUrl
                "Reporting URL host not found in allow list: " + urlParser.toString());
          }

          if (!payload.hasNonNull(Attribute.NORMALIZED_COUNTRY_CODE)) {
            throw new IllegalArgumentException(
                "Missing required payload value " + Attribute.NORMALIZED_COUNTRY_CODE);
          }
          urlParser.addQueryParam(ReportingUrlUtil.PARAM_COUNTRY_CODE,
              payload.get(Attribute.NORMALIZED_COUNTRY_CODE).asText());

          urlParser.addQueryParam(ReportingUrlUtil.PARAM_REGION_CODE,
              attributes.get(Attribute.GEO_SUBDIVISION1));
          urlParser.addQueryParam(ReportingUrlUtil.PARAM_OS_FAMILY,
              getOsParam(attributes.get(Attribute.USER_AGENT_OS)));
          urlParser.addQueryParam(ReportingUrlUtil.PARAM_FORM_FACTOR, "desktop");

          if (message.getAttribute(Attribute.DOCUMENT_TYPE).equals("topsites-click")
              || message.getAttribute(Attribute.DOCUMENT_TYPE).equals("quicksuggest-click")) {
            String userAgentVersion = attributes.get(Attribute.USER_AGENT_VERSION);
            if (userAgentVersion == null) {
              throw new IllegalArgumentException(
                  "Missing required attribute " + Attribute.USER_AGENT_VERSION);
            }
            urlParser.addQueryParam(ReportingUrlUtil.PARAM_PRODUCT_VERSION,
                "firefox_" + userAgentVersion);
          }

          reportingUrl = urlParser.toString();

          attributes.put(Attribute.REPORTING_URL, reportingUrl);
          payload.put(Attribute.REPORTING_URL, reportingUrl);

          return new PubsubMessage(Json.asBytes(payload), attributes);
        }).exceptionsInto(TypeDescriptor.of(PubsubMessage.class))
            .exceptionsVia((ExceptionElement<PubsubMessage> ee) -> {
              try {
                throw ee.exception();
              } catch (UncheckedIOException | IllegalArgumentException
                  | ReportingUrlUtil.InvalidUrlException e) {
                return FailureMessage.of(ParseReportingUrl.class.getSimpleName(), ee.element(),
                    ee.exception());
              }
            }));
  }

  @VisibleForTesting
  boolean isUrlValid(URL url, String doctype) {
    Set<String> allowedUrls;

    if (doctype.endsWith("-click")) {
      allowedUrls = singletonAllowedClickUrls;
    } else if (doctype.endsWith("-impression")) {
      allowedUrls = singletonAllowedImpressionUrls;
    } else {
      throw new IllegalArgumentException("Invalid doctype: " + doctype);
    }

    if (allowedUrls.contains(url.getHost())) {
      return true;
    }

    // check for subdomains (e.g. allow mozilla.test.com but not mozillatest.com)
    return allowedUrls.stream().map(allowedUrl -> "." + allowedUrl)
        .anyMatch(url.getHost()::endsWith);
  }

  /**
   * Return the value used for the os-family parameter of the API
   * associated with the user_agent_os attribute.
   *
   * @throws IllegalArgumentException if the given OS value is not recognized
   */
  private String getOsParam(String userAgentOs) {
    if (userAgentOs == null) {
      throw new IllegalArgumentException("Missing required OS attribute");
    }
    if (userAgentOs.startsWith(OS_WINDOWS)) {
      return PARAM_WINDOWS;
    } else if (userAgentOs.startsWith(OS_MAC)) {
      return PARAM_MAC;
    } else if (userAgentOs.startsWith(OS_LINUX)) {
      return PARAM_LINUX;
    } else {
      throw new IllegalArgumentException("Unrecognized OS attribute: " + userAgentOs);
    }
  }

  /**
   * Returns a mapping from a two-column CSV file where column one
   * is the key and column two is the value.
   *
   * @throws IOException if the given file path cannot be read
   * @throws IllegalArgumentException if the given file location cannot be retrieved
   *     or the CSV format is incorrect
   */
  private List<String[]> readPairsFromFile(ValueProvider<String> fileLocation, String paramName)
      throws IOException {
    if (fileLocation == null || !fileLocation.isAccessible()) {
      throw new IllegalArgumentException("--" + paramName + " argument not accessible");
    }

    try (InputStream inputStream = BeamFileInputStream.open(fileLocation.get());
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader reader = new BufferedReader(inputStreamReader)) {
      List<String[]> pairs = new ArrayList<>();

      while (reader.ready()) {
        String line = reader.readLine();

        if (line != null && !line.isEmpty()) {
          String[] separated = line.split(",");

          if (separated.length != 2) {
            throw new IllegalArgumentException(
                "Invalid mapping: " + line + "; two-column csv expected");
          }

          pairs.add(separated);
        }
      }

      return pairs;
    } catch (IOException e) {
      throw new IOException("Exception thrown while fetching " + paramName, e);
    }
  }

  @VisibleForTesting
  List<Set<String>> loadAllowedUrls() throws IOException {
    if (singletonAllowedImpressionUrls == null || singletonAllowedClickUrls == null) {
      singletonAllowedImpressionUrls = new HashSet<>();
      singletonAllowedClickUrls = new HashSet<>();

      List<String[]> urlToActionTypePairs = readPairsFromFile(urlAllowList, "urlAllowList");

      // 0th element is site, 1st element is action type (click/impression)
      for (String[] urlToActionType : urlToActionTypePairs) {
        if (urlToActionType[1].equals("click")) {
          singletonAllowedClickUrls.add(urlToActionType[0]);
        } else if (urlToActionType[1].equals("impression")) {
          singletonAllowedImpressionUrls.add(urlToActionType[0]);
        } else {
          throw new IllegalArgumentException(
              "Invalid action type in url allow list: " + urlToActionType[1]);
        }
      }
    }
    return Arrays.asList(singletonAllowedClickUrls, singletonAllowedImpressionUrls);
  }
}
