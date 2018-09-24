/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.matchers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import nl.basjes.shaded.org.springframework.core.io.Resource;
import nl.basjes.shaded.org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

/**
 * Utilities for reading files and Java resources and for comparing whether line sets match.
 */
public class Lines {

  /*
   * Matchers.
   */

  /**
   * Return a Hamcrest matcher that checks if a collection contains an expected set of lines.
   *
   * <p>This is a thin wrapper on {@link Matchers#containsInAnyOrder(Object[])} that expands
   * a list argument into an array. The Hamcrest method has a varargs parameter list, so passing
   * a list directly will compile, but treats it as a single entity rather than as a collection
   * of elements.
   */
  public static Matcher<Iterable<? extends String>> matchesInAnyOrder(List<String> expected) {
    return Matchers.containsInAnyOrder(expected.toArray(new String[0]));
  }

  /*
   * Methods for reading content.
   */

  /**
   * Read all files matching the input glob and return lines with no guaranteed order.
   *
   * @see <a href="https://docs.spring.io/spring/docs/5.1.0.RELEASE/javadoc-api/org/springframework/core/io/support/PathMatchingResourcePatternResolver.html">PathMatchingResourcePatternResolver</a>
   */
  public static List<String> files(String glob) {
    return readAllLinesFromGlobWithPrefix("file:" + glob);
  }

  /**
   * Read all Java resources matching the input glob and return lines with no guaranteed order.
   *
   * @see <a href="https://docs.spring.io/spring/docs/5.1.0.RELEASE/javadoc-api/org/springframework/core/io/support/PathMatchingResourcePatternResolver.html">PathMatchingResourcePatternResolver</a>
   */
  public static List<String> resources(String glob) {
    return readAllLinesFromGlobWithPrefix("classpath:" + glob);
  }

  /*
   * Private methods.
   */

  private static List<String> readAllLines(Path path) {
    try {
      return Files.readAllLines(path);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static List<String> readAllLinesFromGlobWithPrefix(String globWithPrefix) {
    List<String> lines = new ArrayList<>();
    try {
      final Resource[] resources =
          new PathMatchingResourcePatternResolver()
              .getResources(globWithPrefix);
      for (Resource resource : resources) {
        readAllLines(resource.getFile().toPath())
            .stream()
            .filter(line -> !line.startsWith("//")) // support comments
            .forEach(lines::add);
      }
      return lines;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

}
