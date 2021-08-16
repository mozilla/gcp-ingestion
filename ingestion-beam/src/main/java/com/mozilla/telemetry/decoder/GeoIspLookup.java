package com.mozilla.telemetry.decoder;

import com.google.common.annotations.VisibleForTesting;
import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.IspResponse;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.BeamFileInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

public class GeoIspLookup
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  public static GeoIspLookup of(String ispDatabase) {
    return new GeoIspLookup(ispDatabase);
  }

  /////////

  private static transient DatabaseReader singletonIspReader;

  private final String geoIspDatabase;

  private GeoIspLookup(String ispDatabase) {
    this.geoIspDatabase = ispDatabase;
  }

  @VisibleForTesting
  static synchronized void clearSingletonsForTests() {
    singletonIspReader = null;
  }

  private static synchronized DatabaseReader getOrCreateSingletonIspReader(String ispDatabase)
      throws IOException {
    if (singletonIspReader == null) {
      File mmdb;

      try {
        InputStream inputStream = BeamFileInputStream.open(ispDatabase);
        Path mmdbPath = Paths.get(System.getProperty("java.io.tmpdir"), "GeoIspLookup.mmdb");
        Files.copy(inputStream, mmdbPath, StandardCopyOption.REPLACE_EXISTING);
        mmdb = mmdbPath.toFile();
      } catch (IOException e) {
        throw new IOException("Exception thrown while fetching configured geoIspDatabase", e);
      }
      singletonIspReader = new DatabaseReader.Builder(mmdb).withCache(new CHMCache()).build();
    }
    return singletonIspReader;
  }

  @VisibleForTesting
  public class Fn extends SimpleFunction<PubsubMessage, PubsubMessage> {

    private transient DatabaseReader ispReader;

    private final Counter foundIp = Metrics.counter(GeoIspLookup.Fn.class, "found_ip");
    private final Counter countIspAlreadyApplied = Metrics.counter(Fn.class, "isp_already_applied");
    private final Counter foundIsp = Metrics.counter(Fn.class, "found_isp");
    private final Counter countIpForwarded = Metrics.counter(GeoIspLookup.Fn.class,
        "ip_from_x_forwarded_for");
    private final Counter countIpRemoteAddr = Metrics.counter(GeoIspLookup.Fn.class,
        "ip_from_remote_addr");

    @Override
    public PubsubMessage apply(PubsubMessage message) {
      message = PubsubConstraints.ensureNonNull(message);

      try {
        if (ispReader == null) {
          loadResourcesOnFirstMessage();
        }

        if (message.getAttributeMap().containsKey(Attribute.ISP_NAME)) {
          // Return early since ISP lookup has already been performed.
          countIspAlreadyApplied.inc();
          return message;
        }

        // copy attributes
        Map<String, String> attributes = new HashMap<String, String>(message.getAttributeMap());

        // Determine client ip
        String ip;
        String xff = attributes.get(Attribute.X_FORWARDED_FOR);

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
          ip = attributes.getOrDefault(Attribute.REMOTE_ADDR, "");
          countIpRemoteAddr.inc();
        }

        try {
          attributes.put(Attribute.ISP_DB_VERSION, DateTimeFormatter.ISO_INSTANT
              .format(Instant.ofEpochMilli(ispReader.getMetadata().getBuildDate().getTime())));

          // Throws UnknownHostException
          InetAddress ipAddress = InetAddress.getByName(ip);
          foundIp.inc();

          IspResponse response = ispReader.isp(ipAddress);
          foundIsp.inc();

          attributes.put(Attribute.ISP_NAME, response.getIsp());
          attributes.put(Attribute.ISP_ORGANIZATION, response.getOrganization());
        } catch (UnknownHostException | GeoIp2Exception ignore) {
          // ignore these exceptions
        }

        // remove null attributes because the coder can't handle them
        attributes.values().removeIf(Objects::isNull);

        return new PubsubMessage(message.getPayload(), attributes);
      } catch (IOException e) {
        // Re-throw unchecked, so that the pipeline will fail at run time if it occurs
        throw new UncheckedIOException(e);
      }
    }

    private void loadResourcesOnFirstMessage() throws IOException {
      if (geoIspDatabase == null) {
        throw new IllegalArgumentException("--geoIspDatabase must be defined for IspLookup");
      }

      ispReader = getOrCreateSingletonIspReader(geoIspDatabase);
    }
  }

  private final GeoIspLookup.Fn fn = new Fn();

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(MapElements.via(fn));
  }
}
