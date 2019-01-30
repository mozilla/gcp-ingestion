/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.google.common.collect.Sets;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import nl.basjes.parse.useragent.UserAgent;
import nl.basjes.parse.useragent.UserAgentAnalyzer;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

/**
 * Extracts browser, version and os information from a user_agent attribute.
 */
public class ParseUserAgent
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  public static ParseUserAgent of() {
    return INSTANCE;
  }

  ////////

  private static class Field implements Serializable {

    private final String attribute;
    private final String field;

    private Field(String attribute, String field) {
      this.attribute = attribute;
      this.field = field;
    }

    private void put(Map<String, String> attributes, UserAgent agent) {
      // Value may be null if not found
      attributes.put(this.attribute, agent.getValue(this.field));
    }
  }

  private static final String USER_AGENT = "user_agent";
  private static final Field USER_AGENT_BROWSER = new Field("user_agent_browser", "AgentName");
  private static final Field USER_AGENT_OS = new Field("user_agent_os", "OperatingSystemName");
  private static final Field USER_AGENT_VERSION = new Field("user_agent_version", "AgentVersion");
  private static final Set<String> parseFailures = Sets.newHashSet("Unknown", "??");

  private static final Fn FN = new Fn();
  private static final ParseUserAgent INSTANCE = new ParseUserAgent();

  private static class Fn extends SimpleFunction<PubsubMessage, PubsubMessage> {

    static final UserAgentAnalyzer analyzer = UserAgentAnalyzer.newBuilder()
        .withField(USER_AGENT_BROWSER.field).withField(USER_AGENT_OS.field)
        .withField(USER_AGENT_VERSION.field)
        // Only use our matchers
        .dropDefaultResources().addResources("UserAgents/*.yaml").build();

    @Override
    public PubsubMessage apply(PubsubMessage message) {
      message = PubsubConstraints.ensureNonNull(message);

      // Copy attributes
      Map<String, String> attributes = new HashMap<String, String>(message.getAttributeMap());

      if (attributes.containsKey(USER_AGENT)) {
        // Extract user agent fields
        UserAgent agent = analyzer.parse(attributes.get(USER_AGENT));

        // Copy agent field to attributes
        USER_AGENT_BROWSER.put(attributes, agent);
        USER_AGENT_OS.put(attributes, agent);
        USER_AGENT_VERSION.put(attributes, agent);

        // Remove null attributes because the coder can't handle them
        attributes.values().removeIf(parseFailures::contains);

        // Remove the original attribute
        attributes.remove(USER_AGENT);
      }
      // Return new message
      return new PubsubMessage(message.getPayload(), attributes);
    }
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(MapElements.via(FN));
  }

  private ParseUserAgent() {
  }

}
