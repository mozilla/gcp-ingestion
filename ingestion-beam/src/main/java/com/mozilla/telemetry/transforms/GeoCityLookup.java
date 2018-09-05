/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.Subdivision;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

public class GeoCityLookup extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {
  private final String geoCityDatabase;

  public GeoCityLookup(String geoCityDatabase) {
    this.geoCityDatabase = geoCityDatabase;
  }

  private class Fn extends SimpleFunction<PubsubMessage, PubsubMessage> {
    private transient DatabaseReader geoIP2City;

    @Override
    public PubsubMessage apply(PubsubMessage value) {
      try {
        if (geoIP2City == null) {
          geoIP2City = new DatabaseReader
              // TODO pull this in a way that allows gs:// paths
              .Builder(new File(geoCityDatabase))
              .withCache(new CHMCache())
              // Throws IOException
              .build();
        }

        // copy attributes
        Map<String, String> attributes = new HashMap<String, String>(value.getAttributeMap());

        // Determine client ip
        String ip;
        if (attributes.containsKey("x_forwarded_for")) {
          String[] ips = attributes.get("x_forwarded_for").split(" *, *");
          ip = ips[ips.length - 1];
        } else {
          ip = attributes.getOrDefault("remote_addr", "");
        }

        try {
          // Throws UnknownHostException
          InetAddress ipAddress = InetAddress.getByName(ip);

          // Throws GeoIp2Exception and IOException
          CityResponse response = geoIP2City.city(ipAddress);

          attributes.put("geo_country", response.getCountry().getIsoCode());
          attributes.put("geo_city", response.getCity().getName());

          List<Subdivision> subdivisions = response.getSubdivisions();
          // Throws IndexOutOfBoundsException
          attributes.put("geo_subdivision1", subdivisions.get(0).getIsoCode());
          attributes.put("geo_subdivision2", subdivisions.get(1).getIsoCode());
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
  }

  private final Fn fn = new Fn();

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(MapElements.via(fn));
  }
}
