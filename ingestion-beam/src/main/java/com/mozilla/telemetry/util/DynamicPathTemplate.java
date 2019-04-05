/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.text.StringSubstitutor;

/**
 * Parses the user-provided --output location into the necessary parts to
 * use FileIO's dynamicWrite capability.
 *
 * <p>We allow the output location to contain placeholders (like {@code ${foo}})
 * with optional defaults ({@code ${foo:-defaultvalue}}) which will be filled
 * in with attributes from the input PubsubMessage's attributeMap.
 * In order to do so, we split the path into a static prefix (provided to FileIO.Write.to())
 * and a dynamic part of the path starting with the first placeholder.
 *
 * <p>For details of the intended behavior for file paths, see:
 * https://github.com/mozilla/gcp-ingestion/tree/master/ingestion-beam#output-path-specification
 */
public class DynamicPathTemplate implements Serializable {

  public final String staticPrefix;
  public final String dynamicPart;
  final List<String> placeholderNames;
  final List<String> placeholderDefaults;

  /** Construct a template from a rawPath as passed in --output. */
  public DynamicPathTemplate(String rawPath) {
    final PathSplitter pathSplitter = new PathSplitter(rawPath);
    this.staticPrefix = pathSplitter.staticPrefix;
    this.dynamicPart = pathSplitter.dynamicPart;
    this.placeholderNames = new ArrayList<>();
    this.placeholderDefaults = new ArrayList<>();

    Matcher matcher = placeholdersPattern.matcher(rawPath);
    while (matcher.find()) {
      String placeholderName = matcher.group(1);
      String placeholderDefault = matcher.group(2);
      if (placeholderDefault == null) {
        throw new IllegalArgumentException("All placeholders in an output path must"
            + " provide defaults using syntax '${placeholderName:-placeholderDefault}'"
            + " but provided path " + rawPath + " contains defaultless placeholder '"
            + matcher.group(0) + "'");
      }
      this.placeholderNames.add(placeholderName);
      this.placeholderDefaults.add(placeholderDefault);
    }

  }

  /** Return the placeholder names in the order of appearance in the original path. */
  public List<String> getPlaceholderNames() {
    return placeholderNames;
  }

  /** Return a mapping of placeholders to their original values. */
  public Map<String, String> getPlaceholderAttributes(List<String> values) {
    if (values.size() != placeholderNames.size()) {
      throw new IllegalArgumentException(String.format(
          "The number of passed values (%d) did not match the number of placeholders (%d)",
          values.size(), placeholderNames.size()));
    }
    Map<String, String> attributes = new HashMap<>(values.size());
    for (int i = 0; i < values.size(); i++) {
      attributes.put(placeholderNames.get(i), values.get(i));
    }
    return attributes;
  }

  /** Return the dynamic part of the output path with placeholders filled in. */
  private String replaceDynamicPart(Map<String, String> attributes) {
    return StringSubstitutor.replace(dynamicPart, attributes);
  }

  /** Return the dynamic part of the output path with placeholders filled in. */
  public String replaceDynamicPart(List<String> values) {
    if (dynamicPart.length() == 0) {
      // Small optimization for the case of non-dynamic paths.
      return dynamicPart;
    } else {
      Map<String, String> attributes = getPlaceholderAttributes(values);
      return replaceDynamicPart(attributes);
    }
  }

  /**
   * Return a list containing just the attribute values needed to fill in the placeholders
   * specified in --output.
   *
   * <p>This is needed to provide a deterministic representation of the
   * attribute map that we can pass through the FileIO.dynamicWrite machinery.
   */
  public List<String> extractValuesFrom(Map<String, String> attributes) {
    if (placeholderNames.isEmpty()) {
      return Collections.emptyList();
    }
    if (attributes == null) {
      attributes = new HashMap<>();
    }
    List<String> placeholderValues = new ArrayList<>(placeholderNames.size());
    for (int i = 0; i < placeholderNames.size(); i++) {
      String name = placeholderNames.get(i);
      String value = attributes.get(name);
      if (value == null) {
        value = placeholderDefaults.get(i);
      }
      placeholderValues.add(value);
    }
    return placeholderValues;
  }

  /**
   * We need some static part of the path to pass to FileIO.Write.to(),
   * as that static piece determines which datastore we're even using.
   * We use as long a prefix as we can and only split on the last '/'
   * we find before we encounter a dynamic attribute placeholder.
   */
  static class PathSplitter implements Serializable {

    final String staticPrefix;
    final String dynamicPart;

    PathSplitter(String rawPath) {
      int indexOfFirstPlaceholder = rawPath.indexOf("${");

      int indexOfLastSlash;
      if (indexOfFirstPlaceholder == -1) {
        indexOfLastSlash = rawPath.lastIndexOf('/');
      } else {
        indexOfLastSlash = rawPath.substring(0, indexOfFirstPlaceholder).lastIndexOf('/');
      }

      if (indexOfLastSlash == -1) {
        this.staticPrefix = "";
        this.dynamicPart = rawPath;
      } else {
        this.staticPrefix = rawPath.substring(0, indexOfLastSlash) + "/";
        this.dynamicPart = rawPath.substring(indexOfLastSlash + 1);
      }
    }
  }

  private static Pattern placeholdersPattern = Pattern.compile(
      // A placeholder without a default looks like ${foo}
      // A placeholder with a default looks like ${foo:-defaultvalue}
      // This regex matches either of these patterns,
      // capturing the name of the placeholder as group 1
      // and the default value (or null if no default is provided) as group 2.
      "\\$\\{([^{}]+?(?=:-|}))(?::-([^{}]+))?}");

}
