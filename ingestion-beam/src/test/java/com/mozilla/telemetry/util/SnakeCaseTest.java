package com.mozilla.telemetry.util;

import static org.junit.Assert.assertEquals;

import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.junit.Test;

public class SnakeCaseTest {

  @Test
  public void testSnakeCaseFormat() {
    // one word
    assertEquals(SnakeCase.format("Aa"), "aa");
    // two words
    assertEquals(SnakeCase.format("aA"), "a_a");
    // underscores are word boundaries
    assertEquals(SnakeCase.format("_a__a_"), "a_a");
    // mnemonics are words
    assertEquals(SnakeCase.format("RAM"), "ram");
    // digits can be lowercase
    assertEquals(SnakeCase.format("a7aAa"), "a7a_aa");
    // digits can be uppercase
    assertEquals(SnakeCase.format("A7AAa"), "a7a_aa");
  }

  void runFileTest(String path) {
    String input = Resources.getResource(path).getPath();
    try (Stream<String> stream = Files.lines(Paths.get(input))) {
      stream.forEach(s -> {
        String[] split = s.split(",", 2);
        String reference = split[0];
        String expected = split[1];
        assertEquals(SnakeCase.format(reference), expected);
      });
    } catch (IOException e) {
      assert (false);
    }

  }

  @Test
  public void testSnakeCaseFormatAlphaNum3() {
    // all strings of length 3 drawn from the alphabet "aA7"
    runFileTest("casing/alphanum_3.csv");
  }

  @Test
  public void testSnakeCaseFormatWord4() {
    // all strings of length 4 drawn from the alphabet "aA7_"
    runFileTest("casing/word_4.csv");
  }

  @Test
  public void testSnakeCaseFormatMpsDiffIntegration() {
    // all column names from mozilla-pipeline-schemas affected by snake_casing
    runFileTest("casing/mps-diff-integration.csv");
  }

}
