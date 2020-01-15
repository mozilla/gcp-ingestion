package com.mozilla.telemetry.decoder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Subdivision;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
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

  public static GeoCityLookup of(ValueProvider<String> geoCityDatabase,
      ValueProvider<String> geoCityFilter) {
    return new GeoCityLookup(geoCityDatabase, geoCityFilter);
  }

  /////////

  private static final Pattern GEO_NAME_PATTERN = Pattern.compile("^(\\d+).*");

  private static transient DatabaseReader singletonGeoCityReader;
  private static transient Set<Integer> singletonAllowedCities;

  private final ValueProvider<String> geoCityDatabase;
  private final ValueProvider<String> geoCityFilter;

  private GeoCityLookup(ValueProvider<String> geoCityDatabase,
      ValueProvider<String> geoCityFilter) {
    this.geoCityDatabase = geoCityDatabase;
    this.geoCityFilter = geoCityFilter;
  }

  @VisibleForTesting
  static synchronized void clearSingletonsForTests() {
    singletonGeoCityReader = null;
    singletonAllowedCities = null;
  }

  /**
   * Returns a singleton object for reading from the GeoCity database.
   *
   * <p>We copy the configured database file to a static temp location so that the MaxMind API can
   * save on heap usage by using memory mapping. The reader is threadsafe and this singleton pattern
   * allows multiple worker threads on the same machine to share a single reader instance.
   *
   * <p>Note that we do not clean up the temp mmdb file, but it's a static path, so running locally
   * will overwrite the existing path every time rather than creating an unbounded number of copies.
   * This also assumes that only one JVM per machine is running this code. In the production case
   * where this is running on Cloud Dataflow, we should always have a clean environment and the temp
   * state will be cleaned up along with the workers once the job finishes. However, behavior is
   * undefined if you run multiple local jobs concurrently.
   *
   * @throws IOException if the configured file path is not a valid .mmdb file
   */
  private static synchronized DatabaseReader getOrCreateSingletonGeoCityReader(
      ValueProvider<String> geoCityDatabase) throws IOException {
    if (singletonGeoCityReader == null) {
      File mmdb;
      try {
        InputStream inputStream;
        Metadata metadata = FileSystems.matchSingleFileSpec(geoCityDatabase.get());
        ReadableByteChannel channel = FileSystems.open(metadata.resourceId());
        inputStream = Channels.newInputStream(channel);
        Path mmdbPath = Paths.get(System.getProperty("java.io.tmpdir"), "GeoCityLookup.mmdb");
        Files.copy(inputStream, mmdbPath, StandardCopyOption.REPLACE_EXISTING);
        mmdb = mmdbPath.toFile();
      } catch (IOException e) {
        throw new IOException("Exception thrown while fetching configured geoCityDatabase", e);
      }
      singletonGeoCityReader = new DatabaseReader.Builder(mmdb).withCache(new CHMCache()).build();
    }
    return singletonGeoCityReader;
  }

  /**
   * Returns a singleton object describing allowed cities.
   *
   * @throws IOException if the configured file path does not exist or is in a bad format
   */
  private static synchronized Set<Integer> getOrCreateSingletonAllowedCities(
      ValueProvider<String> geoCityFilter) throws IOException {
    if (singletonAllowedCities == null) {
      InputStream inputStream;
      try {
        Metadata metadata = FileSystems.matchSingleFileSpec(geoCityFilter.get());
        ReadableByteChannel channel = FileSystems.open(metadata.resourceId());
        inputStream = Channels.newInputStream(channel);
      } catch (IOException e) {
        throw new IOException("Exception thrown while fetching configured geoCityFilter", e);
      }
      singletonAllowedCities = new HashSet<>();
      BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
      while (reader.ready()) {
        String line = reader.readLine();
        Matcher matcher = GEO_NAME_PATTERN.matcher(line);
        if (matcher.find()) {
          Integer geoNameId = Integer.valueOf(matcher.group(1));
          singletonAllowedCities.add(geoNameId);
        } else {
          throw new IllegalStateException(
              "Line of geoCityFilter file does not begin with a geoName integer ID: " + line);

        }
      }
    }
    return singletonAllowedCities;
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

        if (message.getAttributeMap().containsKey(Attribute.GEO_COUNTRY)) {
          // Return early since geo has already been applied;
          // we are likely reprocessing a message from error output.
          countGeoAlreadyApplied.inc();
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
          attributes.put("geo_db_version", DateTimeFormatter.ISO_INSTANT
              .format(Instant.ofEpochMilli(geoIP2City.getMetadata().getBuildDate().getTime())));

          // Throws UnknownHostException
          InetAddress ipAddress = InetAddress.getByName(ip);
          foundIp.inc();

          // Throws GeoIp2Exception and IOException
          CityResponse response = geoIP2City.city(ipAddress);
          foundCity.inc();

          String countryCode = response.getCountry().getIsoCode();
          attributes.put(Attribute.GEO_COUNTRY, countryCode);

          City city = response.getCity();
          if (cityAllowed(city.getGeoNameId())) {
            attributes.put(Attribute.GEO_CITY, city.getName());
            foundCityAllowed.inc();
          }

          List<Subdivision> subdivisions = response.getSubdivisions();
          // Throws IndexOutOfBoundsException
          attributes.put(Attribute.GEO_SUBDIVISION1, subdivisions.get(0).getIsoCode());
          foundGeo1.inc();
          attributes.put(Attribute.GEO_SUBDIVISION2, subdivisions.get(1).getIsoCode());
          foundGeo2.inc();

        } catch (UnknownHostException | GeoIp2Exception | IndexOutOfBoundsException ignore) {
          // ignore these exceptions
        }

        // remove client ip from attributes
        attributes.remove(Attribute.X_FORWARDED_FOR);
        attributes.remove(Attribute.REMOTE_ADDR);

        // remove null attributes because the coder can't handle them
        attributes.values().removeIf(Objects::isNull);

        return new PubsubMessage(message.getPayload(), attributes, message.getMessageId());
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
      geoIP2City = getOrCreateSingletonGeoCityReader(geoCityDatabase);
      if (geoCityFilter != null && geoCityFilter.isAccessible()
          && !Strings.isNullOrEmpty(geoCityFilter.get())) {
        allowedCities = getOrCreateSingletonAllowedCities(geoCityFilter);
      }
    }
  }

  private final Fn fn = new Fn();

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(MapElements.via(fn));
  }

}
