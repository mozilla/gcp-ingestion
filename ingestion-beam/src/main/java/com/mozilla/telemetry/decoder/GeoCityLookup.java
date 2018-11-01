/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.google.common.annotations.VisibleForTesting;
import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.Subdivision;
import com.mozilla.telemetry.util.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

public class GeoCityLookup
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  private final String geoCityDatabase;

  public GeoCityLookup(String geoCityDatabase) {
    this.geoCityDatabase = geoCityDatabase;
  }

  @VisibleForTesting
  public class Fn extends SimpleFunction<PubsubMessage, PubsubMessage> {

    private transient DatabaseReader geoIP2City;

    private final Counter countIpForwarded = Metrics.counter(Fn.class, "ip-from-x-forwarded-for");
    private final Counter countIpRemoteAddr = Metrics.counter(Fn.class, "ip-from-remote-addr");
    private final Counter foundIp = Metrics.counter(Fn.class, "found-ip");
    private final Counter foundCity = Metrics.counter(Fn.class, "found-city");
    private final Counter foundGeo1 = Metrics.counter(Fn.class, "found-geo-subdivision-1");
    private final Counter foundGeo2 = Metrics.counter(Fn.class, "found-geo-subdivision-2");

    @Override
    public PubsubMessage apply(PubsubMessage value) {
      try {
        if (geoIP2City == null) {
          // Throws IOException
          loadGeoIp2City();
        }

        // copy attributes
        Map<String, String> attributes = new HashMap<String, String>(value.getAttributeMap());

        // Determine client ip
        String ip;
        if (attributes.containsKey("x_forwarded_for")) {
          String[] ips = attributes.get("x_forwarded_for").split(" *, *");
          ip = ips[ips.length - 1];
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
          attributes.put("geo_city", response.getCity().getName());

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

        return new PubsubMessage(value.getPayload(), attributes);
      } catch (IOException e) {
        // Re-throw unchecked, so that the pipeline will fail at run time if it occurs
        throw new UncheckedIOException(e);
      }
    }

    private void loadGeoIp2City() throws IOException {
      InputStream inputStream;
      try {
        inputStream = File.inputStream(geoCityDatabase);
      } catch (IOException e) {
        throw new IllegalArgumentException(
            "Exception thrown while fetching configured geoCityDatabase", e);
      }
      geoIP2City = new DatabaseReader.Builder(inputStream).withCache(new CHMCache()).build();
    }
  }

  private final Fn fn = new Fn();

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(MapElements.via(fn));
  }

}
