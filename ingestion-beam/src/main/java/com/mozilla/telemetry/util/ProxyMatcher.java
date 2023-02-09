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

  /////////

  private static final Pattern GOOGLE_VERSION_LINE = Pattern
      .compile("((goog|cloud)[.]json) published: (.*)$");

  public final String version;
  private final List<IPAddress> proxies;

  private ProxyMatcher(String listGoogleNonCustomerIps, String listCloudFrontIps) {
    final List<String> addresses = new ArrayList<>();
    final List<String> versions = new ArrayList<>();
    if (!Strings.isNullOrEmpty(listGoogleNonCustomerIps)) {
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
            addresses.add(line);
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Exception thrown while fetching Google subnets", e);
      }
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
      cloudFront.get("prefixes").elements().forEachRemaining(node -> {
        addresses.add(node.get("ip_prefix").asText());
      });
      cloudFront.get("ipv6_prefixes").elements().forEachRemaining(node -> {
        addresses.add(node.get("ipv6_prefix").asText());
      });
      versions.add("cloudfront:" + cloudFront.get("syncToken").asText() + ":"
          + cloudFront.get("createDate").asText());
    }

    this.proxies = addresses.stream()
        .map(addrString -> new IPAddressString(addrString).getAddress())
        .collect(Collectors.toList());
    if (versions.isEmpty()) {
      version = null;
    } else {
      version = String.join(";", versions);
    }
  }

  /** Check whether address is from any proxies. */
  public boolean apply(String address) {
    final IPAddress ipAddress = new IPAddressString(address).getAddress();
    if (ipAddress == null) {
      return false;
    }
    return proxies.stream().anyMatch(subnet -> subnet.contains(ipAddress));
  }
}
