package com.mozilla.telemetry.ingestion.core.util;

import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SnakeCase {

  private SnakeCase() {
  }

  private static Pattern REV_WORD_BOUND_PAT = Pattern.compile("" //
      + "\\b" // standard word boundary
      + "|(?<=[a-z][A-Z])(?=\\d*[A-Z])" // A7Aa -> A7|Aa boundary
      + "|(?<=[a-z][A-Z])(?=\\d*[a-z])" // a7Aa -> a7|Aa boundary
      + "|(?<=[A-Z])(?=\\d*[a-z])"); // a7A -> a7|A boundary

  /**
   * Convert a name to snake case.
   *
   * <p>The specific implementation here uses regular expressions in order to be compatible across
   * languages. See https://github.com/acmiyaguchi/test-casing
   */
  public static String format(String input) {
    String subbed = new StringBuilder(input).reverse().toString().replaceAll("[^\\w]|_", " ");
    String reversedResult = Arrays.stream(REV_WORD_BOUND_PAT.split(subbed)) //
        .map(String::trim) //
        .map(String::toLowerCase) //
        .filter(s -> s.length() > 0) //
        .collect(Collectors.joining("_"));
    return new StringBuilder(reversedResult).reverse().toString();
  }
}
