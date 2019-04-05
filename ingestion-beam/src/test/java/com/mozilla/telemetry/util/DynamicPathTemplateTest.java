/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.util;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.util.DynamicPathTemplate.PathSplitter;
import java.util.Arrays;
import org.junit.Test;

public class DynamicPathTemplateTest {

  @Test
  public void testSplittingPrefixFromDynamicPath() {
    final PathSplitter absoluteStaticPath = new PathSplitter("/tmp/abcd/foo/test");
    assertEquals("/tmp/abcd/foo/", absoluteStaticPath.staticPrefix);
    assertEquals("test", absoluteStaticPath.dynamicPart);

    final PathSplitter absoluteDynamicPath = new PathSplitter("/tmp/abcd/bar-${foo:-f}/test");
    assertEquals("/tmp/abcd/", absoluteDynamicPath.staticPrefix);
    assertEquals("bar-${foo:-f}/test", absoluteDynamicPath.dynamicPart);

    final PathSplitter relativeDynamicPath = new PathSplitter("bar-${foo:-f}/test");
    assertEquals("", relativeDynamicPath.staticPrefix);
    assertEquals("bar-${foo:-f}/test", relativeDynamicPath.dynamicPart);

    final PathSplitter gcsPath = new PathSplitter("gs://mybucket/abcd/bar-${foo:-f}/test");
    assertEquals("gs://mybucket/abcd/", gcsPath.staticPrefix);
    assertEquals("bar-${foo:-f}/test", gcsPath.dynamicPart);
  }

  @Test
  public void testParsePlaceholderNames() {
    assertThat(new DynamicPathTemplate("gs://mybucket/abcd/bar-${foo:-f}/test").placeholderNames,
        is(ImmutableList.of("foo")));

    assertThat(new DynamicPathTemplate("tmp/${bar:-b}-${foo:-f}/test").placeholderNames,
        is(ImmutableList.of("bar", "foo")));

    assertThat(new DynamicPathTemplate("tmp/test").placeholderNames, is(ImmutableList.of()));

    assertThat(new DynamicPathTemplate("tmp/${foo_1:-bar_1}/${foo-2:-bar-2}").placeholderNames,
        is(ImmutableList.of("foo_1", "foo-2")));

    assertThat(new DynamicPathTemplate("tmp/${foo:baz-baz:-mydefault:-hi}").placeholderNames,
        is(ImmutableList.of("foo:baz-baz")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyDefaultThrows() {
    assertThat(new DynamicPathTemplate("tmp/${bar}-${foo:-hi}/${baz}-test").placeholderDefaults,
        is(Arrays.asList(null, "hi", null)));
  }

  @Test
  public void testParsePlaceholderDefaults() {
    assertThat(new DynamicPathTemplate("tmp/${foo_1:-bar_1}/${foo-2:-bar-2}").placeholderDefaults,
        is(ImmutableList.of("bar_1", "bar-2")));

    assertThat(new DynamicPathTemplate("tmp/${foo:baz-baz:-mydefault:-hi}").placeholderDefaults,
        is(ImmutableList.of("mydefault:-hi")));
  }

  @Test
  public void testReplaceDynamicPart() {
    final DynamicPathTemplate path = new DynamicPathTemplate("tmp/${bar:-b}-${foo:-f}/test");
    assertEquals("hi-there/test", path.replaceDynamicPart(ImmutableList.of("hi", "there")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReplaceDynamicPartThrows() {
    final DynamicPathTemplate path = new DynamicPathTemplate("tmp/${bar:-b}-${foo:-f}/test");
    assertEquals("hi-there/test", path.replaceDynamicPart(ImmutableList.of("hi")));
  }

  @Test
  public void testExtractValuesFrom() {
    final DynamicPathTemplate path = new DynamicPathTemplate("tmp/${bar:-b}-${foo:-f}/test");
    final ImmutableMap<String, String> attributes = ImmutableMap.of("bar", "hi", "foo", "there",
        "unused", "blah");
    assertThat(path.extractValuesFrom(attributes), is(ImmutableList.of("hi", "there")));
    assertThat(path.extractValuesFrom(null), is(ImmutableList.of("b", "f")));
  }

  @Test
  public void testWithoutDynamicPart() {
    final DynamicPathTemplate path = new DynamicPathTemplate("tmp/");
    assertEquals("", path.replaceDynamicPart(ImmutableList.of()));
  }

  @Test
  public void testPlaceholderNamesGetter() {
    final DynamicPathTemplate path = new DynamicPathTemplate("${foo:-f}/${bar:-b}/${baz:-bz}");
    assertEquals(ImmutableList.of("foo", "bar", "baz"), path.getPlaceholderNames());
  }

  @Test
  public void testGetPlaceholderAttributes() {
    final DynamicPathTemplate path = new DynamicPathTemplate("${foo:-f}/${bar:-b}/${baz:-bz}");
    final ImmutableList<String> values = ImmutableList.of("one", "two", "three");
    final ImmutableMap<String, String> expect = ImmutableMap.of("foo", "one", "bar", "two", "baz",
        "three");
    assertEquals(expect, path.getPlaceholderAttributes(values));
  }

}
