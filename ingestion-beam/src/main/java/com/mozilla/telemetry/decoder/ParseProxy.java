package com.mozilla.telemetry.decoder;

import com.google.common.annotations.VisibleForTesting;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.ProxyMatcher;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;

public class ParseProxy extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  public static ParseProxy of(String cloudFrontSubnetList, String googleSubnetList) {
    return new ParseProxy(cloudFrontSubnetList, googleSubnetList);
  }

  /////////

  private static ProxyMatcher singletonProxyMatcher;

  private final String listGoogleNonCustomerIps;
  private final String listCloudFrontIps;

  private ParseProxy(String listGoogleNonCustomerIps, String listCloudFrontIps) {
    this.listGoogleNonCustomerIps = listGoogleNonCustomerIps;
    this.listCloudFrontIps = listCloudFrontIps;
  }

  @VisibleForTesting
  static synchronized void clearSingletonsForTests() {
    singletonProxyMatcher = null;
  }

  private static synchronized ProxyMatcher getOrCreateSingletonProxyMatcher(
      String listGoogleNonCustomerIps, String listCloudFrontIps) {
    if (singletonProxyMatcher == null) {
      singletonProxyMatcher = ProxyMatcher.of(listGoogleNonCustomerIps, listCloudFrontIps);
    }
    return singletonProxyMatcher;
  }

  @VisibleForTesting
  class Fn extends DoFn<PubsubMessage, PubsubMessage> {

    private transient ProxyMatcher proxyMatcher;

    private final Counter countProxySubnet = Metrics.counter(Fn.class, "proxy_subnet");
    private final Counter countProxySubnetAlreadyApplied = Metrics.counter(Fn.class,
        "proxy_subnet_already_applied");
    private final Distribution teeLatencyTimer = Metrics.distribution(Fn.class,
        "tee_latency_millis");

    @Setup
    public void setup() {
      proxyMatcher = getOrCreateSingletonProxyMatcher(listGoogleNonCustomerIps, listCloudFrontIps);
    }

    @ProcessElement
    public void processElement(@Element PubsubMessage rawMessage,
        OutputReceiver<PubsubMessage> out) {
      // Prevent null pointer exception
      final PubsubMessage message = PubsubConstraints.ensureNonNull(rawMessage);

      // Copy attributes
      final Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

      Optional.ofNullable(attributes.get(Attribute.X_FORWARDED_FOR))
          .map(v -> Arrays.stream(v.split("\\s*,\\s*")).filter(StringUtils::isNotBlank)
              .collect(Collectors.toList()))
          .ifPresent(xff -> {
            final int xffOriginalSize = xff.size();

            if (message.getAttributeMap().containsKey(Attribute.PROXY_LIST_VERSIONS)) {
              // Skip proxy detection since it has already been applied;
              // we are likely reprocessing a message from error output.
              countProxySubnetAlreadyApplied.inc();
            } else {
              attributes.put(Attribute.PROXY_LIST_VERSIONS, proxyMatcher.version);

              // Google's load balancer will append the immediate sending client IP and a global
              // forwarding rule IP to any existing content in X-Forwarded-For as documented in:
              // https://cloud.google.com/load-balancing/docs/https/#components
              //
              // Our nginx setup will then append google's load balancer IP.
              //
              // In practice, many of the "first" addresses are bogus or internal, so remove load
              // balancer entries and downstream transforms will read the immediate sending client
              // IP from the end of the list.
              if (xff.size() > 1) {
                // remove last two indices
                xff.remove(xff.size() - 1);
                xff.remove(xff.size() - 1);
              }

              // If any of the configured proxy subnets are present, then remove the proxy IP. See:
              // https://bugzilla.mozilla.org/show_bug.cgi?id=1789992
              final int xffSize = xff.size();
              while (xff.size() > 0) {
                final ProxyMatcher.ProxySource pt = proxyMatcher.apply(xff.get(xff.size() - 1));
                if (pt == null) {
                  break; // stop if no proxy is detected
                }
                xff.remove(xff.size() - 1); // remove last entry when proxy is detected
                if (ProxyMatcher.ProxySource.GOOGLE == pt) {
                  xff.remove(xff.size() - 1); // remove an additional entry for google proxy
                }
              }
              if (xffSize != xff.size()) {
                // only count once per message
                countProxySubnet.inc();
                attributes.put(Attribute.PROXY_DETECTED, "true");
              }
            }

            // Update attributes if xff changed
            if (xffOriginalSize != xff.size()) {
              attributes.put(Attribute.X_FORWARDED_FOR, String.join(",", xff));
            }
          });

      // remove unused ip from attributes
      attributes.remove(Attribute.REMOTE_ADDR);

      // Remove null attributes because the coder can't handle them.
      attributes.values().removeIf(Objects::isNull);

      // Return new message.
      out.output(new PubsubMessage(message.getPayload(), attributes));
    }
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(ParDo.of(new Fn()));
  }
}
