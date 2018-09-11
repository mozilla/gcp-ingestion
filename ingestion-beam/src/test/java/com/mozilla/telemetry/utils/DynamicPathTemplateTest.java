package com.mozilla.telemetry.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.utils.DynamicPathTemplate.PathSplitter;
import java.util.Arrays;
import org.junit.Test;

public class DynamicPathTemplateTest {

  @Test
  public void testSplittingPrefixFromDynamicPath() {
    final PathSplitter absoluteStaticPath = new PathSplitter(
        "/tmp/abcd/foo/test");
    assertEquals("/tmp/abcd/foo/test", absoluteStaticPath.staticPrefix);
    assertEquals("", absoluteStaticPath.dynamicPart);

    final PathSplitter absoluteDynamicPath = new PathSplitter(
        "/tmp/abcd/bar-${foo}/test");
    assertEquals("/tmp/abcd/", absoluteDynamicPath.staticPrefix);
    assertEquals("bar-${foo}/test", absoluteDynamicPath.dynamicPart);

    final PathSplitter relativeDynamicPath = new PathSplitter(
        "bar-${foo}/test");
    assertEquals("", relativeDynamicPath.staticPrefix);
    assertEquals("bar-${foo}/test", relativeDynamicPath.dynamicPart);

    final PathSplitter gcsPath = new PathSplitter(
        "gs://mybucket/abcd/bar-${foo}/test");
    assertEquals("gs://mybucket/abcd/", gcsPath.staticPrefix);
    assertEquals("bar-${foo}/test", gcsPath.dynamicPart);
  }

  @Test
  public void testParsePlaceholderNames() {
    assertThat(
        new DynamicPathTemplate("gs://mybucket/abcd/bar-${foo}/test").placeholderNames,
        is(ImmutableList.of("foo")));

    assertThat(
        new DynamicPathTemplate("tmp/${bar}-${foo}/test").placeholderNames,
        is(ImmutableList.of("bar", "foo")));

    assertThat(
        new DynamicPathTemplate("tmp/test").placeholderNames,
        is(ImmutableList.of()));
  }

  @Test
  public void testParsePlaceholderDefaults() {
    assertThat(
        new DynamicPathTemplate("tmp/${bar}-${foo:-hi}/${baz}-test").placeholderDefaults,
        is(Arrays.asList(null, "hi", null)));
  }

  @Test
  public void testReplaceDynamicPart() {
    final DynamicPathTemplate path = new DynamicPathTemplate("tmp/${bar}-${foo}/test");
    assertEquals("hi-there/test", path.replaceDynamicPart(ImmutableList.of("hi", "there")));
  }

  @Test
  public void testExtractValuesFrom() {
    final DynamicPathTemplate path = new DynamicPathTemplate("tmp/${bar}-${foo}/test");
    final ImmutableMap<String, String> attributes = ImmutableMap.of(
        "bar", "hi",
        "foo", "there",
        "unused", "blah"
    );
    assertThat(
        ImmutableList.of("hi", "there"),
        is(path.extractValuesFrom(attributes)));
  }

}
