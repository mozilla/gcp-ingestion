/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.google.common.annotations.VisibleForTesting;
import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Subdivision;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
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
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

public class GeoCityLookup
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  private static final Pattern GEO_NAME_PATTERN = Pattern.compile("^(\\d+).*");

  private final String geoCityDatabase;
  private final String geoCityFilter;

  public GeoCityLookup(String geoCityDatabase, String geoCityFilter) {
    this.geoCityDatabase = geoCityDatabase;
    this.geoCityFilter = geoCityFilter;
  }

  @VisibleForTesting
  public class Fn extends SimpleFunction<PubsubMessage, PubsubMessage> {

    private transient DatabaseReader geoIP2City;
    private transient Set<Integer> allowedCities;

    private final Counter countIpForwarded = Metrics.counter(Fn.class, "ip-from-x-forwarded-for");
    private final Counter countIpRemoteAddr = Metrics.counter(Fn.class, "ip-from-remote-addr");
    private final Counter countPipelineProxy = Metrics.counter(Fn.class, "count-pipeline-proxy");
    private final Counter foundIp = Metrics.counter(Fn.class, "found-ip");
    private final Counter foundCity = Metrics.counter(Fn.class, "found-city");
    private final Counter foundCityAllowed = Metrics.counter(Fn.class, "found-city-allowed");
    private final Counter foundGeo1 = Metrics.counter(Fn.class, "found-geo-subdivision-1");
    private final Counter foundGeo2 = Metrics.counter(Fn.class, "found-geo-subdivision-2");

    @Override
    public PubsubMessage apply(PubsubMessage value) {
      try {
        if (geoIP2City == null) {
          // Throws IOException
          loadGeoIp2City();
          if (geoCityFilter != null) {
            loadAllowedCities();
          }
        }

        // copy attributes
        Map<String, String> attributes = new HashMap<String, String>(value.getAttributeMap());

        // Determine client ip
        String ip;
        if (attributes.containsKey("x_forwarded_for")) {
          String[] ips = attributes.get("x_forwarded_for").split(" *, *");
          if (attributes.containsKey("x_pipeline_proxy")) {
            // Use the ip reported by the proxy load balancer
            ip = ips[ips.length - 2];
            countPipelineProxy.inc();
          } else {
            // Use the ip reported by the ingestion-edge load balancer
            ip = ips[ips.length - 1];
          }
          countIpForwarded.inc();
        } else {
          ip = attributes.getOrDefault("remote_addr", "");
          countIpRemoteAddr.inc();
        }

        try {
          // Throws UnknownHostException
          InetAddress ipAddress = InetAddress.getByName(ip);
          foundIp.inc();

          // Throws GeoIp2Exception and IOException
          CityResponse response = geoIP2City.city(ipAddress);
          foundCity.inc();

          attributes.put("geo_country", response.getCountry().getIsoCode());

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
        attributes.remove("x_pipeline_proxy");
        attributes.remove("remote_addr");

        // remove null attributes because the coder can't handle them
        attributes.values().removeIf(Objects::isNull);

        return new PubsubMessage(value.getPayload(), attributes);
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

    private void loadGeoIp2City() throws IOException {
      InputStream inputStream;
      try {
        Metadata metadata = FileSystems.matchSingleFileSpec(geoCityDatabase);
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
        Metadata metadata = FileSystems.matchSingleFileSpec(geoCityFilter);
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
