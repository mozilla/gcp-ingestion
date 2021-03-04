package com.mozilla.telemetry.contextualservices;

import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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
  private final ValueProvider<String> countryIpList;
  private final ValueProvider<String> osUserAgentList;

  private static transient Set<String> singletonAllowedImpressionUrls;
  private static transient Set<String> singletonAllowedClickUrls;
  private static transient Map<String, String> singletonCountryToIpMapping;
  private static transient Map<String, String> singletonOsToUserAgentMapping;

  private static final String DEFAULT_COUNTRY = "US";
  private static final String DEFAULT_OS = "Windows";

  public static ParseReportingUrl of(ValueProvider<String> urlAllowList,
      ValueProvider<String> countryIpList, ValueProvider<String> osUserAgentList) {
    return new ParseReportingUrl(urlAllowList, countryIpList, osUserAgentList);
  }

  private ParseReportingUrl(ValueProvider<String> urlAllowList, ValueProvider<String> countryIpList,
      ValueProvider<String> osUserAgentList) {
    this.urlAllowList = urlAllowList;
    this.countryIpList = countryIpList;
    this.osUserAgentList = osUserAgentList;
  }

  private static class InvalidUrlException extends RuntimeException {

    InvalidUrlException(String message) {
      super(message);
    }

    public InvalidUrlException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  @Override
  public Result<PCollection<PubsubMessage>, PubsubMessage> expand(
      PCollection<PubsubMessage> messages) {
    return messages.apply(
        MapElements.into(TypeDescriptor.of(PubsubMessage.class)).via((PubsubMessage message) -> {
          message = PubsubConstraints.ensureNonNull(message);

          ObjectNode json;
          try {
            json = Json.readObjectNode(message.getPayload());
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }

          try {
            loadAllowedUrls();
            loadCountryToIpMapping();
            loadOsUserAgentMapping();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }

          Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

          String reportingUrl = json.path(Attribute.REPORTING_URL).asText();

          URL urlObj;
          try {
            urlObj = new URL(reportingUrl);
          } catch (MalformedURLException e) {
            throw new InvalidUrlException("Could not parse reporting URL: " + reportingUrl, e);
          }

          if (urlObj.getHost() == null || urlObj.getHost().isEmpty()) {
            throw new InvalidUrlException("Reporting URL null or missing: " + reportingUrl);
          }

          if (!isUrlValid(urlObj, attributes.get(Attribute.DOCUMENT_TYPE))) {
            PerDocTypeCounter.inc(attributes, "RejectedNonNullUrl");
            throw new InvalidUrlException(
                "Reporting URL host not found in allow list: " + reportingUrl);
          }

          // Get query parameters as map
          Map<String, String> queryParams = Arrays.stream(urlObj.getQuery().split("&"))
              .map(param -> param.split("="))
              .collect(Collectors.toMap(item -> item[0], item -> item[1]));

          queryParams.put("ip", createIpParam(attributes.get(Attribute.NORMALIZED_COUNTRY_CODE)));

          try {
            queryParams.put("ua", createUserAgentParam(attributes.get(Attribute.USER_AGENT_OS),
                attributes.get(Attribute.USER_AGENT_VERSION)));
          } catch (UnsupportedEncodingException e) {
            throw new UncheckedIOException(e);
          }

          // Generate query string from map
          String queryString = queryParams.entrySet().stream().map(
              entry -> entry.getKey() + (entry.getValue() == null ? "" : "=" + entry.getValue()))
              .collect(Collectors.joining("&"));

          try {
            reportingUrl = new URL(urlObj.getProtocol(), urlObj.getHost(),
                urlObj.getPath() + "?" + queryString).toString();
          } catch (MalformedURLException e) {
            throw new InvalidUrlException(
                "Could not parse reporting with query string: " + queryString, e);
          }

          attributes.put(Attribute.REPORTING_URL, reportingUrl);
          json.put(Attribute.REPORTING_URL, reportingUrl);

          return new PubsubMessage(Json.asBytes(json), attributes);
        }).exceptionsInto(TypeDescriptor.of(PubsubMessage.class))
            .exceptionsVia((ExceptionElement<PubsubMessage> ee) -> {
              try {
                throw ee.exception();
              } catch (UncheckedIOException | IllegalArgumentException | InvalidUrlException e) {
                return FailureMessage.of(ParseReportingUrl.class.getSimpleName(), ee.element(),
                    ee.exception());
              }
            }));
  }

  private boolean isUrlValid(URL url, String doctype) {
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

  private String createIpParam(String countryCode) {
    String ipParam = singletonCountryToIpMapping.getOrDefault(countryCode,
        singletonCountryToIpMapping.get(DEFAULT_COUNTRY));
    if (ipParam == null) {
      throw new IllegalArgumentException("Could not get ip value: Unrecognized country "
          + "and missing default value: " + Attribute.NORMALIZED_COUNTRY_CODE);
    }
    return ipParam;
  }

  private String createUserAgentParam(String os, String clientVersion)
      throws UnsupportedEncodingException {
    String normalizedOs;
    if (os.startsWith("Windows")) {
      normalizedOs = "Windows";
    } else if (os.startsWith("Macintosh")) {
      normalizedOs = "Macintosh";
    } else if (os.startsWith("Linux")) {
      normalizedOs = "Linux";
    } else {
      normalizedOs = DEFAULT_OS;
    }
    String userAgentFormatString = singletonOsToUserAgentMapping.getOrDefault(normalizedOs,
        singletonOsToUserAgentMapping.get(DEFAULT_COUNTRY));
    if (userAgentFormatString == null) {
      throw new IllegalArgumentException(
          "Could not get user agent value: Unrecognized OS and missing default value: " + os);
    }
    String userAgent = MessageFormat.format(userAgentFormatString, clientVersion);
    return URLEncoder.encode(userAgent, "UTF-8");
  }

  /**
   * Returns a mapping from a two-column CSV file where column one
   * is the key and column two is the value.
   *
   * @throws IOException if the given file path cannot be read
   * @throws IllegalArgumentException if the given file location cannot be retrieved
   *     or the CSV format is incorrect
   */
  private Map<String, String> readMappingFromFile(ValueProvider<String> fileLocation,
      String paramName) throws IOException {
    if (fileLocation == null || !fileLocation.isAccessible()) {
      throw new IllegalArgumentException("--" + paramName + " argument not found");
    }

    try (InputStream inputStream = BeamFileInputStream.open(fileLocation.get());
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader reader = new BufferedReader(inputStreamReader)) {
      Map<String, String> mapping = new HashMap<>();

      while (reader.ready()) {
        String line = reader.readLine();

        if (line != null && !line.isEmpty()) {
          String[] separated = line.split(",");

          if (separated.length != 2) {
            throw new IllegalArgumentException(
                "Invalid mapping: " + line + "; two-column csv expected");
          }
          mapping.put(separated[0], separated[1]);
        }
      }

      return mapping;
    } catch (IOException e) {
      throw new IOException("Exception thrown while fetching " + paramName, e);
    }
  }

  private Map<String, String> loadCountryToIpMapping() throws IOException {
    if (singletonCountryToIpMapping == null) {
      singletonCountryToIpMapping = readMappingFromFile(countryIpList, "countryIpList");
    }
    return singletonCountryToIpMapping;
  }

  private Map<String, String> loadOsUserAgentMapping() throws IOException {
    if (singletonOsToUserAgentMapping == null) {
      singletonOsToUserAgentMapping = readMappingFromFile(osUserAgentList, "osUserAgentList");
    }
    return singletonOsToUserAgentMapping;
  }

  private void loadAllowedUrls() throws IOException {
    if (singletonAllowedImpressionUrls == null || singletonAllowedClickUrls == null) {
      singletonAllowedImpressionUrls = new HashSet<>();
      singletonAllowedClickUrls = new HashSet<>();

      Map<String, String> urlToActionTypeMap = readMappingFromFile(urlAllowList, "urlAllowList");

      for (Map.Entry<String, String> urlToActionType : urlToActionTypeMap.entrySet()) {
        if (urlToActionType.getValue().equals("click")) {
          singletonAllowedClickUrls.add(urlToActionType.getKey());
        } else if (urlToActionType.getValue().equals("impression")) {
          singletonAllowedImpressionUrls.add(urlToActionType.getKey());
        } else {
          throw new IllegalArgumentException(
              "Invalid action type in url allow list: " + urlToActionType.getValue());
        }
      }
    }
  }
}
