/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Subdivision;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

public class GeoCityLookup
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  public static final String GEO_COUNTRY = "geo_country";

  public static GeoCityLookup of(ValueProvider<String> geoCityDatabase,
      ValueProvider<String> geoCityFilter) {
    return new GeoCityLookup(geoCityDatabase, geoCityFilter);
  }

  /////////

  private static final Pattern GEO_NAME_PATTERN = Pattern.compile("^(\\d+).*");

  private final ValueProvider<String> geoCityDatabase;
  private final ValueProvider<String> geoCityFilter;

  private GeoCityLookup(ValueProvider<String> geoCityDatabase,
      ValueProvider<String> geoCityFilter) {
    this.geoCityDatabase = geoCityDatabase;
    this.geoCityFilter = geoCityFilter;
  }

  @VisibleForTesting
  public class Fn extends SimpleFunction<PubsubMessage, PubsubMessage> {

    private transient DatabaseReader geoIP2City;
    private transient Set<Integer> allowedCities;

    private final Counter countGeoAlreadyApplied = Metrics.counter(Fn.class, "geo_already_applied");
    private final Counter countIpForwarded = Metrics.counter(Fn.class, "ip_from_x_forwarded_for");
    private final Counter countIpRemoteAddr = Metrics.counter(Fn.class, "ip_from_remote_addr");
    private final Counter foundIp = Metrics.counter(Fn.class, "found_ip");
    private final Counter foundCity = Metrics.counter(Fn.class, "found_city");
    private final Counter foundCityAllowed = Metrics.counter(Fn.class, "found_city_allowed");
    private final Counter foundGeo1 = Metrics.counter(Fn.class, "found_geo_subdivision_1");
    private final Counter foundGeo2 = Metrics.counter(Fn.class, "found_geo_subdivision_2");

    @Override
    public PubsubMessage apply(PubsubMessage message) {
      message = PubsubConstraints.ensureNonNull(message);
      try {
        if (geoIP2City == null) {
          // Throws IOException
          loadResourcesOnFirstMessage();
        }

        if (message.getAttributeMap().containsKey("geo_country")) {
          // Return early since geo has already been applied;
          // we are likely reprocessing a message from error output.
          countGeoAlreadyApplied.inc();
          return message;
        }

        // copy attributes
        Map<String, String> attributes = new HashMap<String, String>(message.getAttributeMap());

        // Determine client ip
        String ip;
        String xff = attributes.get("x_forwarded_for");
        if (xff != null) {
          // Google's load balancer will append the immediate sending client IP and a global
          // forwarding rule IP to any existing content in X-Forwarded-For as documented in:
          // https://cloud.google.com/load-balancing/docs/https/#components
          //
          // In practice, many of the "first" addresses are bogus or internal,
          // so we target the immediate sending client IP.
          String[] ips = xff.split("\\s*,\\s*");
          ip = ips[Math.max(ips.length - 2, 0)];
          countIpForwarded.inc();
        } else {
          ip = attributes.getOrDefault("remote_addr", "");
          countIpRemoteAddr.inc();
        }

        try {
          attributes.put("geo_db_version", DateTimeFormatter.ISO_INSTANT
              .format(Instant.ofEpochMilli(geoIP2City.getMetadata().getBuildDate().getTime())));

          // Throws UnknownHostException
          InetAddress ipAddress = InetAddress.getByName(ip);
          foundIp.inc();

          // Throws GeoIp2Exception and IOException
          CityResponse response = geoIP2City.city(ipAddress);
          foundCity.inc();

          String countryCode = response.getCountry().getIsoCode();
          attributes.put(GEO_COUNTRY, countryCode);

          City city = response.getCity();
          if (cityAllowed(city.getGeoNameId())) {
            attributes.put("geo_city", city.getName());
            foundCityAllowed.inc();
          }

          List<Subdivision> subdivisions = response.getSubdivisions();
          // Throws IndexOutOfBoundsException
          attributes.put("geo_subdivision1", subdivisions.get(0).getIsoCode());
          foundGeo1.inc();
          attributes.put("geo_subdivision2", subdivisions.get(1).getIsoCode());
          foundGeo2.inc();

        } catch (UnknownHostException | GeoIp2Exception | IndexOutOfBoundsException ignore) {
          // ignore these exceptions
        }

        // remove client ip from attributes
        attributes.remove("x_forwarded_for");
        attributes.remove("remote_addr");

        // remove null attributes because the coder can't handle them
        attributes.values().removeIf(Objects::isNull);

        return new PubsubMessage(message.getPayload(), attributes);
      } catch (IOException e) {
        // Re-throw unchecked, so that the pipeline will fail at run time if it occurs
        throw new UncheckedIOException(e);
      }
    }

    private boolean cityAllowed(Integer geoNameId) {
      if (geoNameId == null) {
        return false;
      } else if (allowedCities == null) {
        return true;
      } else {
        return allowedCities.contains(geoNameId);
      }
    }

    private void loadResourcesOnFirstMessage() throws IOException {
      if (geoCityDatabase == null || !geoCityDatabase.isAccessible()) {
        throw new IllegalArgumentException("--geoCityDatabase must be defined for GeoCityLookup!");
      }
      loadGeoIp2City();
      if (geoCityFilter != null && geoCityFilter.isAccessible()
          && !Strings.isNullOrEmpty(geoCityFilter.get())) {
        loadAllowedCities();
      }
    }

    private void loadGeoIp2City() throws IOException {
      InputStream inputStream;
      try {
        Metadata metadata = FileSystems.matchSingleFileSpec(geoCityDatabase.get());
        ReadableByteChannel channel = FileSystems.open(metadata.resourceId());
        inputStream = Channels.newInputStream(channel);
      } catch (IOException e) {
        throw new IOException("Exception thrown while fetching configured geoCityDatabase", e);
      }
      geoIP2City = new DatabaseReader.Builder(inputStream).withCache(new CHMCache()).build();
    }

    private void loadAllowedCities() throws IOException {
      InputStream inputStream;
      try {
        Metadata metadata = FileSystems.matchSingleFileSpec(geoCityFilter.get());
        ReadableByteChannel channel = FileSystems.open(metadata.resourceId());
        inputStream = Channels.newInputStream(channel);
      } catch (IOException e) {
        throw new IOException("Exception thrown while fetching configured geoCityFilter", e);
      }
      allowedCities = new HashSet<>();
      BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
      while (reader.ready()) {
        String line = reader.readLine();
        Matcher matcher = GEO_NAME_PATTERN.matcher(line);
        if (matcher.find()) {
          Integer geoNameId = Integer.valueOf(matcher.group(1));
          allowedCities.add(geoNameId);
        } else {
          throw new IllegalStateException(
              "Line of geoCityFilter file does not begin with a geoName integer ID: " + line);

        }
      }
    }
  }

  private final Fn fn = new Fn();

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(MapElements.via(fn));
  }

}
