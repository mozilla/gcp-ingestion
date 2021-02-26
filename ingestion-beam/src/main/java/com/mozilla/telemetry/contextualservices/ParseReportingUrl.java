package com.mozilla.telemetry.contextualservices;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.BeamFileInputStream;
import com.mozilla.telemetry.util.Json;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
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

  private static transient Set<String> singletonAllowedUrls;
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
            if (singletonAllowedUrls == null) {
              loadAllowedUrls();
            }

            if (singletonCountryToIpMapping == null) {
              loadCountryToIpMapping();
            }

            if (singletonOsToUserAgentMapping == null) {
              loadOsUserAgentMapping();
            }
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }

          String reportingUrl = json.path(Attribute.REPORTING_URL).asText();

          URL urlObj;
          try {
            urlObj = new URL(reportingUrl);
          } catch (MalformedURLException e) {
            throw new InvalidUrlException("Could not parse reporting URL: " + reportingUrl, e);
          }

          // TODO: this is placeholder logic
          // if (urlObj.getHost() == "" || !singletonAllowedUrls.contains(urlObj.getHost())) {
          // throw new InvalidUrlException("Reporting URL host not found in allow list: "
          // + reportingUrl);
          // }

          Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

          // Get query params as map
          Map<String, String> queryParams = Arrays.stream(urlObj.getQuery().split("&"))
              .map(param -> param.split("="))
              .collect(Collectors.toMap(item -> item[0], item -> item[1]));

          String ipParam = singletonCountryToIpMapping.getOrDefault(
              attributes.get(Attribute.NORMALIZED_COUNTRY_CODE),
              singletonCountryToIpMapping.get(DEFAULT_COUNTRY));
          if (ipParam == null) {
            throw new IllegalArgumentException("Could not get ip value: Unrecognized country "
                + "and missing default value: " + Attribute.NORMALIZED_COUNTRY_CODE);
          }
          queryParams.put("ip", ipParam);

          String os = attributes.get(Attribute.USER_AGENT_OS);
          String clientVersion = attributes.get(Attribute.USER_AGENT_VERSION);
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
          String userAgent = String.format(singletonOsToUserAgentMapping.getOrDefault(normalizedOs,
              singletonOsToUserAgentMapping.get(DEFAULT_COUNTRY)), clientVersion);
          if (userAgent == null) {
            throw new IllegalArgumentException(
                "Could not get user agent value: Unrecognized OS and missing default value: " + os);
          }
          queryParams.put("ua", userAgent);

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

          return new PubsubMessage(message.getPayload(), attributes);
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

  private Set<String> loadAllowedUrls() throws IOException {
    if (urlAllowList == null || !urlAllowList.isAccessible()) {
      throw new IllegalArgumentException("--urlAllowList not found");
    }

    if (singletonAllowedUrls == null) {
      try (InputStream inputStream = BeamFileInputStream.open(urlAllowList.get());
          InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
          BufferedReader reader = new BufferedReader(inputStreamReader)) {
        singletonAllowedUrls = new HashSet<>();

        while (reader.ready()) {
          String line = reader.readLine();

          if (line != null && !line.isEmpty()) {
            singletonAllowedUrls.add(line);
          }
        }
      } catch (IOException e) {
        throw new IOException("Exception thrown while fetching urlAllowList", e);
      }
    }
    return singletonAllowedUrls;
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
      throw new IOException("Exception thrown while fetching countryToIpMapping", e);
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
}
