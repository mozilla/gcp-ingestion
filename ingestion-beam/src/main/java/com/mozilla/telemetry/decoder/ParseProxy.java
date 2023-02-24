package com.mozilla.telemetry.decoder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.ProxyMatcher;
import com.mozilla.telemetry.util.Time;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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

  public static ParseProxy of(String proxyIps, String cloudFrontSubnetList,
      String googleSubnetList) {
    return new ParseProxy(proxyIps, cloudFrontSubnetList, googleSubnetList);
  }

  /////////

  private static ProxyMatcher singletonProxyMatcher;

  private final String listGoogleNonCustomerIps;
  private final String listCloudFrontIps;
  private final Set<String> proxyIps;

  private ParseProxy(String proxyIps, String listGoogleNonCustomerIps, String listCloudFrontIps) {
    this.listGoogleNonCustomerIps = listGoogleNonCustomerIps;
    this.listCloudFrontIps = listCloudFrontIps;
    if (!Strings.isNullOrEmpty(proxyIps)) {
      this.proxyIps = new HashSet<>(Arrays.asList(proxyIps.split("\\s*,\\s*")));
    } else {
      this.proxyIps = new HashSet<>();
    }
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

    private final Counter countPipelineProxy = Metrics.counter(Fn.class, "pipeline_proxy");
    private final Counter countProxyIpAddress = Metrics.counter(Fn.class, "proxy_ip_address");
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
    public void processElement(@Element PubsubMessage message, OutputReceiver<PubsubMessage> out) {
      // Prevent null pointer exception
      message = PubsubConstraints.ensureNonNull(message);

      // Copy attributes
      Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

      final String xpp = attributes.get(Attribute.X_PIPELINE_PROXY);
      final Optional<List<String>> xff = Optional
          .ofNullable(attributes.get(Attribute.X_FORWARDED_FOR))
          .map(v -> Arrays.stream(v.split("\\s*,\\s*")).filter(StringUtils::isNotBlank)
              .collect(Collectors.toList()));
      final Integer xffOriginalSize = xff.map(List::size).orElse(0);

      // If any of the configured proxy IPs are present, then there's a GCP load balancer IP we will
      // need to remove. This must be done before handling X-Pipeline-Proxy header because values
      // are expected to appear between the AWS tee and the edge. See
      // https://bugzilla.mozilla.org/show_bug.cgi?id=1729069#c8
      xff.ifPresent(xffList -> {
        xffList.removeIf(proxyIps::contains);
        if (xffOriginalSize != xffList.size()) {
          countProxyIpAddress.inc();
        }
      });

      // If the X-Pipeline-Proxy header is present, this message came from the AWS tee and
      // we need to make some modifications to headers.
      if (xpp != null) {

        // Check if X-Pipeline-Proxy is a timestamp
        final Instant proxyInstant = Time.parseAsInstantOrNull(xpp);
        if (proxyInstant != null) {
          // Record the difference between submission and proxy times as tee latency.
          final String submissionTimestamp = attributes.get(Attribute.SUBMISSION_TIMESTAMP);
          final Instant submissionInstant = Time.parseAsInstantOrNull(submissionTimestamp);
          if (submissionInstant != null) {
            teeLatencyTimer.update(submissionInstant.toEpochMilli() - proxyInstant.toEpochMilli());
          }
          // Rename submission timestamp to proxy timestamp
          attributes.put(Attribute.PROXY_TIMESTAMP, submissionTimestamp);
          // Use submission timestamp from X-Pipeline-Proxy
          attributes.put(Attribute.SUBMISSION_TIMESTAMP, xpp);
        }

        xff.ifPresent(xffList -> {
          if (xffList.size() > 1) {
            // remove next to last index
            xffList.remove(xffList.size() - 2);
          }
        });

        // Remove the proxy attribute
        attributes.remove(Attribute.X_PIPELINE_PROXY);

        // Report proxied message
        countPipelineProxy.inc();
      }

      if (message.getAttributeMap().containsKey(Attribute.PROXY_LIST_VERSIONS)) {
        // Skip proxy detection since it has already been applied;
        // we are likely reprocessing a message from error output.
        countProxySubnetAlreadyApplied.inc();
      } else {
        attributes.put(Attribute.PROXY_LIST_VERSIONS, proxyMatcher.version);

        // If any of the configured proxy subnets are present, then there's a proxy IP we will need
        // to remove. This must be done after handling X-Pipeline-Proxy header because values are
        // expected to appear before the AWS tee. See
        // https://bugzilla.mozilla.org/show_bug.cgi?id=1789992
        xff.ifPresent(xffList -> {
          final int xffListSize = xffList.size();
          xffList.removeIf(proxyMatcher::apply);
          if (xffListSize != xffList.size()) {
            countProxySubnet.inc();
            attributes.put(Attribute.PROXY_DETECTED, "true");
          }
        });
      }

      // Update attributes if xff changed
      xff.ifPresent(xffList -> {
        if (xffOriginalSize != xffList.size()) {
          attributes.put(Attribute.X_FORWARDED_FOR, String.join(",", xffList));
        }
      });

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
