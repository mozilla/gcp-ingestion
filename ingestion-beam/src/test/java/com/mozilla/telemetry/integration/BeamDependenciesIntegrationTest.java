package com.mozilla.telemetry.integration;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class BeamDependenciesIntegrationTest {

  @Test
  public void checkVersions() throws Exception {
    final String beamVersion = System.getProperty("beam.version");
    final Pom beamCore = getPom("org.apache.beam", "beam-sdks-java-core", beamVersion);

    final Map<String, Optional<String>> expectedVersions = ImmutableMap.of(//
        "avro.version", beamCore.getVersion("org.apache.avro", "avro"), //
        "jackson.version", beamCore.getVersion("com.fasterxml.jackson.core", "jackson-core"));

    final Map<String, Optional<String>> actualVersions = ImmutableMap.of(//
        "avro.version", Optional.of(System.getProperty("avro.version")), //
        "jackson.version", Optional.of(System.getProperty("jackson.version")));

    assertEquals(expectedVersions, actualVersions);
  }

  // Helper methods

  private Pom getPom(String groupId, String artifactId, String version) throws Exception {
    return mapper.readValue(new URL("https://repo1.maven.org/maven2/" + groupId.replace('.', '/')
        + "/" + artifactId + "/" + version + "/" + artifactId + "-" + version + ".pom"), Pom.class);
  }

  private XmlMapper mapper = new XmlMapper();

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Pom {

    @JacksonXmlProperty
    public List<Dependency> dependencies = ImmutableList.of();

    @JacksonXmlProperty
    public DependencyManagement dependencyManagement = new DependencyManagement();

    /** Get dependency version. */
    public Optional<String> getVersion(String groupId, String artifactId) {
      return Streams.concat(dependencies.stream(), dependencyManagement.dependencies.stream())
          .filter(d -> groupId.equals(d.groupId) && artifactId.equals(d.artifactId))
          .map(d -> d.version).findFirst();
    }
  }

  public static class DependencyManagement {

    @JacksonXmlProperty
    public List<Dependency> dependencies = ImmutableList.of();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Dependency {

    @JacksonXmlProperty
    private String groupId;

    @JacksonXmlProperty
    private String artifactId;

    @JacksonXmlProperty
    public String version;
  }
}
