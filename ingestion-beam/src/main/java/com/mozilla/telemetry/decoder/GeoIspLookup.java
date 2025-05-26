package com.mozilla.telemetry.decoder;

import com.google.common.annotations.VisibleForTesting;
import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.AnonymousIpResponse;
import com.maxmind.geoip2.model.EnterpriseResponse;
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

  public static GeoIspLookup of(String ispDatabase, String enterpriseDatabase,
      String anonymousIpDatabase) {
    return new GeoIspLookup(ispDatabase, enterpriseDatabase, anonymousIpDatabase);
  }

  // TODO: remove and update tests
  public static GeoIspLookup of(String ispDatabase) {
    return new GeoIspLookup(ispDatabase, null, null);
  }

  /////////

  private static transient DatabaseReader singletonIspReader;
  private static transient DatabaseReader singletonEnterpriseReader;
  private static transient DatabaseReader singletonAnonymousReader;

  private final String geoIspDatabase;
  private final String enterpriseDatabase;
  private final String anonymousIpDatabase;

  private GeoIspLookup(String ispDatabase, String enterpriseDatabase, String anonymousIpDatabase) {
    this.geoIspDatabase = ispDatabase;
    this.enterpriseDatabase = enterpriseDatabase;
    this.anonymousIpDatabase = anonymousIpDatabase;
  }

  @VisibleForTesting
  static synchronized void clearSingletonsForTests() {
    singletonIspReader = null;
  }

  private static synchronized DatabaseReader getOrCreateDatabaseReader(String fileLocation,
      String outputName) throws IOException {
    File mmdb;

    try {
      InputStream inputStream = BeamFileInputStream.open(fileLocation);
      Path mmdbPath = Paths.get(System.getProperty("java.io.tmpdir"),
          String.format("%s.mmdb", outputName));
      Files.copy(inputStream, mmdbPath, StandardCopyOption.REPLACE_EXISTING);
      mmdb = mmdbPath.toFile();
    } catch (IOException e) {
      throw new IOException(
          String.format("Exception thrown while fetching configured %s", outputName), e);
    }

    return new DatabaseReader.Builder(mmdb).withCache(new CHMCache()).build();
  }

  private static synchronized DatabaseReader getOrCreateSingletonIspReader(String ispDatabase)
      throws IOException {
    if (singletonIspReader == null) {
      singletonIspReader = getOrCreateDatabaseReader(ispDatabase, "GeoIspLookup");
    }
    return singletonIspReader;
  }

  private static synchronized DatabaseReader getOrCreateSingletonEnterpriseReader(
      String enterpriseReader) throws IOException {
    if (singletonEnterpriseReader == null) {
      singletonEnterpriseReader = getOrCreateDatabaseReader(enterpriseReader,
          "GeoEnterpriseLookup");
    }
    return singletonEnterpriseReader;
  }

  private static synchronized DatabaseReader getOrCreateSingletonAnonymousReader(
      String anonymousReader) throws IOException {
    if (singletonAnonymousReader == null) {
      singletonAnonymousReader = getOrCreateDatabaseReader(anonymousReader, "GeoAnonymousLookup");
    }
    return singletonAnonymousReader;
  }

  @VisibleForTesting
  public class Fn extends SimpleFunction<PubsubMessage, PubsubMessage> {

    private transient DatabaseReader ispReader;
    private transient DatabaseReader enterpriseReader;
    private transient DatabaseReader anonymousReader;

    private final Counter foundIp = Metrics.counter(GeoIspLookup.Fn.class, "found_ip");
    private final Counter countIspAlreadyApplied = Metrics.counter(Fn.class, "isp_already_applied");
    private final Counter foundIsp = Metrics.counter(Fn.class, "found_isp");
    private final Counter foundEnterpriseIsp = Metrics.counter(Fn.class, "found_enterprise_isp");
    private final Counter foundAnonymousIp = Metrics.counter(Fn.class, "found_anonymous_ip");

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
        String xff = attributes.get(Attribute.X_FORWARDED_FOR);

        if (xff != null) {
          // In practice, many of the "first" addresses are bogus or internal,
          // so we target the immediate sending client IP by choosing the last entry.
          String[] ips = xff.split("\\s*,\\s*");
          String ip = ips[Math.max(ips.length - 1, 0)];

          attributes.put(Attribute.ISP_DB_VERSION, DateTimeFormatter.ISO_INSTANT
              .format(Instant.ofEpochMilli(ispReader.getMetadata().getBuildDate().getTime())));

          InetAddress ipAddress = null;
          try {
            ipAddress = InetAddress.getByName(ip);
            foundIp.inc();
          } catch (UnknownHostException ignore) {
            // ignore these exceptions
          }

          if (ipAddress != null) {
            try {
              IspResponse ispResponse = ispReader.isp(ipAddress);
              foundIsp.inc();

              attributes.put(Attribute.ISP_NAME, ispResponse.getIsp());
              attributes.put(Attribute.ISP_ORGANIZATION, ispResponse.getOrganization());
            } catch (GeoIp2Exception ignore) {
              // ignore these exceptions
            }

            try {
              EnterpriseResponse enterpriseResponse = enterpriseReader.enterprise(ipAddress);
              foundEnterpriseIsp.inc();

              attributes.put(Attribute.ISP_USER_TYPE, enterpriseResponse.getTraits().getUserType());
              attributes.put(Attribute.ISP_CONNECTION_TYPE,
                  enterpriseResponse.getTraits().getConnectionType() == null ? null
                      : enterpriseResponse.getTraits().getConnectionType().toString());
            } catch (GeoIp2Exception ignore) {
              // ignore these exceptions
            }

            try {
              AnonymousIpResponse anonymousResponse = anonymousReader.anonymousIp(ipAddress);
              foundAnonymousIp.inc();

              attributes.put(Attribute.ISP_IS_ANONYMOUS,
                  Boolean.toString(anonymousResponse.isAnonymous()));
              attributes.put(Attribute.ISP_IS_ANONYMOUS_VPN,
                  Boolean.toString(anonymousResponse.isAnonymousVpn()));
              attributes.put(Attribute.ISP_IS_HOSTING_PROVIDER,
                  Boolean.toString(anonymousResponse.isHostingProvider()));
              attributes.put(Attribute.ISP_IS_PUBLIC_PROXY,
                  Boolean.toString(anonymousResponse.isPublicProxy()));
              attributes.put(Attribute.ISP_IS_RESIDENTIAL_PROXY,
                  Boolean.toString(anonymousResponse.isResidentialProxy()));
              attributes.put(Attribute.ISP_IS_TOR_EXIT_NODE,
                  Boolean.toString(anonymousResponse.isTorExitNode()));
            } catch (GeoIp2Exception ignore) {
              // ignore these exceptions
            }
          }
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

      if (enterpriseDatabase != null) {
        enterpriseReader = getOrCreateSingletonEnterpriseReader(enterpriseDatabase);
      }

      if (anonymousIpDatabase != null) {
        anonymousReader = getOrCreateSingletonAnonymousReader(anonymousIpDatabase);
      }
    }
  }

  private final GeoIspLookup.Fn fn = new Fn();

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(MapElements.via(fn));
  }
}
