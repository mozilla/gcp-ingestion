package com.mozilla.telemetry.contextualservices;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for parsing and building contextual services reporting URLs.
 */
public class BuildReportingUrl {

  // API parameter names
  static final String PARAM_COUNTRY_CODE = "country-code";
  static final String PARAM_REGION_CODE = "region-code";
  static final String PARAM_OS_FAMILY = "os-family";
  // Slot-number is 1-indexed: the first item will have the index 1, not zero
  static final String PARAM_POSITION = "slot-number";
  static final String PARAM_FORM_FACTOR = "form-factor";
  static final String PARAM_PRODUCT_VERSION = "product-version";
  static final String PARAM_ID = "id";
  static final String PARAM_IMPRESSIONS = "impressions";
  static final String PARAM_TIMESTAMP_BEGIN = "begin-timestamp";
  static final String PARAM_TIMESTAMP_END = "end-timestamp";
  static final String PARAM_CLICK_STATUS = "click-status";
  static final String PARAM_IMPRESSION_STATUS = "custom-data";
  static final String PARAM_DMA_CODE = "dma-code";
  static final String PARAM_CUSTOM_DATA = "custom-data";

  private final URL reportingUrl;
  private final Map<String, String> queryParams;

  static class InvalidUrlException extends RuntimeException {

    InvalidUrlException(String message) {
      super(message);
    }

    public InvalidUrlException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  BuildReportingUrl(String reportingUrl) {
    try {
      this.reportingUrl = new URL(reportingUrl);
    } catch (MalformedURLException e) {
      throw new InvalidUrlException("Could not parse reporting URL: " + reportingUrl, e);
    }

    if (this.reportingUrl.getHost() == null || this.reportingUrl.getHost().isEmpty()) {
      throw new InvalidUrlException("Reporting URL null or missing: " + reportingUrl);
    }

    try {
      if (this.reportingUrl.getQuery() == null) {
        queryParams = new HashMap<>();
      } else {
        queryParams = Arrays.stream(this.reportingUrl.getQuery().split("&"))
            .map(param -> param.split("="))
            .collect(Collectors.toMap(item -> item[0], item -> item.length > 1 ? item[1] : ""));
      }
    } catch (IllegalStateException e) {
      throw new InvalidUrlException("Reporting URL contains duplicate query keys: " + reportingUrl);
    }
  }

  public void addQueryParam(String name, String value) {
    queryParams.put(name, value);
  }

  public String getQueryParam(String paramName) {
    return queryParams.get(paramName);
  }

  public Map<String, String> getQueryParams() {
    return queryParams;
  }

  public String getBaseUrl() {
    return reportingUrl.toString().split("\\?")[0];
  }

  /**
   * Build reporting url by creating query param string (sorted by key).
   */
  public URL getReportingUrl() {
    // Generate query string from map sorted by key
    String queryString = queryParams
        .entrySet().stream().sorted(Map.Entry.comparingByKey()).map(entry -> String.format("%s=%s",
            entry.getKey(), entry.getValue() == null ? "" : entry.getValue()))
        .collect(Collectors.joining("&"));

    try {
      return new URL(getBaseUrl() + "?" + queryString);
    } catch (MalformedURLException e) {
      throw new InvalidUrlException(
          "Could not parse URL " + getBaseUrl() + " with query string: " + queryString, e);
    }
  }

  @Override
  public String toString() {
    return getReportingUrl().toString();
  }
}
