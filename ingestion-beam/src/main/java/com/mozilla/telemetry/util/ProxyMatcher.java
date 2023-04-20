package com.mozilla.telemetry.util;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileSystems;

public class ProxyMatcher {

  public static ProxyMatcher of(String listGoogleNonCustomerIps, String listCloudFrontIps) {
    return new ProxyMatcher(listGoogleNonCustomerIps, listCloudFrontIps);
  }

  public enum ProxySource {
    GOOGLE, CLOUD_FRONT
  }

  /////////

  private static final Pattern GOOGLE_VERSION_LINE = Pattern
      .compile("((goog|cloud)[.]json) published: (.*)$");

  public final String version;
  private final List<IPAddress> googleProxies;
  private final List<IPAddress> cloudFrontProxies;

  private ProxyMatcher(String listGoogleNonCustomerIps, String listCloudFrontIps) {
    final List<String> versions = new ArrayList<>();
    if (!Strings.isNullOrEmpty(listGoogleNonCustomerIps)) {
      final List<String> googleAddresses = new ArrayList<>();
      try {
        final ReadableByteChannel channel = FileSystems
            .open(FileSystems.matchSingleFileSpec(listGoogleNonCustomerIps).resourceId());
        final InputStream inputStream = Channels.newInputStream(channel);
        final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        while (reader.ready()) {
          String line = reader.readLine();
          Matcher matcher = GOOGLE_VERSION_LINE.matcher(line);
          if (matcher.find()) {
            // add version info from header
            versions.add(matcher.group(1) + ":" + matcher.group(3));
          } else if (new IPAddressString(line).getAddress() != null) {
            // add subnet
            googleAddresses.add(line);
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Exception thrown while fetching Google subnets", e);
      }
      googleProxies = googleAddresses.stream().map(a -> new IPAddressString(a).getAddress())
          .collect(Collectors.toList());
    } else {
      googleProxies = List.of();
    }

    if (!Strings.isNullOrEmpty(listCloudFrontIps)) {
      final ObjectNode cloudFront;
      try {
        cloudFront = Json.readObjectNode(//
            Channels.newInputStream(//
                FileSystems.open(FileSystems.matchSingleFileSpec(listCloudFrontIps).resourceId()))
                .readAllBytes());
      } catch (IOException e) {
        throw new UncheckedIOException("Exception thrown while fetching CloudFront subnets", e);
      }
      versions.add("cloudfront:" + cloudFront.get("syncToken").asText() + ":"
          + cloudFront.get("createDate").asText());
      final List<String> cloudFrontAddresses = new ArrayList<>();
      cloudFront.get("prefixes").elements().forEachRemaining(node -> {
        cloudFrontAddresses.add(node.get("ip_prefix").asText());
      });
      cloudFront.get("ipv6_prefixes").elements().forEachRemaining(node -> {
        cloudFrontAddresses.add(node.get("ipv6_prefix").asText());
      });
      cloudFrontProxies = cloudFrontAddresses.stream()
          .map(addrString -> new IPAddressString(addrString).getAddress())
          .collect(Collectors.toList());
    } else {
      cloudFrontProxies = List.of();
    }

    if (versions.isEmpty()) {
      // must not be null, because presence is used to detect whether ParseProxy has been applied
      version = "";
    } else {
      version = String.join(";", versions);
    }
  }

  /** Check whether address is from any proxies, and return the number of entries to remove from
   *  X-Forwarded-For.
   */
  public ProxySource apply(String address) {
    final IPAddress ipAddress = new IPAddressString(address).getAddress();
    if (ipAddress != null) {
      if (googleProxies.stream().anyMatch(subnet -> subnet.contains(ipAddress))) {
        return ProxySource.GOOGLE;
      }
      if (cloudFrontProxies.stream().anyMatch(subnet -> subnet.contains(ipAddress))) {
        return ProxySource.CLOUD_FRONT;
      }
    }
    return null;
  }
}
