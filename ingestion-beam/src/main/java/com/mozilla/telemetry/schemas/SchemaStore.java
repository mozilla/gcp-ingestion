/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.schemas;

import avro.shaded.com.google.common.annotations.VisibleForTesting;
import com.mozilla.telemetry.util.Json;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

/** Reads schemas from a file and parses them into a map. */
public class SchemaStore implements Serializable {

  /** Returns a SchemaStore based on the contents of the archive at schemasLocation. */
  public static SchemaStore of(ValueProvider<String> schemasLocation) {
    return new SchemaStore(schemasLocation);
  }

  /** Return the parsed schema corresponding to a path underneath the schemas/ directory. */
  public Schema getSchema(String path) throws SchemaNotFoundException {
    ensureSchemasLoaded();
    Schema schema = schemas.get(path);
    if (schema == null) {
      throw SchemaNotFoundException.forName(path);
    }
    return schema;
  }

  /** Return the parsed schema corresponding to doctype and namespace parsed from attributes. */
  public Schema getSchema(Map<String, String> attributes) throws SchemaNotFoundException {
    ensureSchemasLoaded();
    if (attributes == null) {
      throw new SchemaNotFoundException("No schema for message with null attributeMap");
    }
    // This is the path provided by mozilla-pipeline-schemas
    final String path = StringSubstitutor.replace("${document_namespace}/${document_type}/"
        + "${document_type}.${document_version}.schema.json", attributes);
    return getSchema(path);
  }

  /** Returns true if we found at least one schema with the given doctype and namespace. */
  public boolean docTypeExists(String namespace, String docType) {
    ensureSchemasLoaded();
    if (namespace == null || docType == null) {
      return false;
    } else {
      return dirs.contains(namespace + "/" + docType);
    }
  }

  /** Returns true if we found at least one schema with matching the given attributes. */
  public boolean docTypeExists(Map<String, String> attributes) {
    String namespace = attributes.get("document_namespace");
    String docType = attributes.get("document_type");
    return docTypeExists(namespace, docType);
  }

  ////////

  private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(SchemaStore.class);

  private final ValueProvider<String> schemasLocation;
  private transient Map<String, Schema> schemas;
  private transient Set<String> dirs;

  private SchemaStore(ValueProvider<String> schemasLocation) {
    this.schemasLocation = schemasLocation;
  }

  @VisibleForTesting
  int numLoadedSchemas() {
    ensureSchemasLoaded();
    return schemas.size();
  }

  private void loadAllSchemas() throws IOException {
    final Map<String, Schema> tempSchemas = new HashMap<>();
    final Set<String> tempDirs = new HashSet<>();
    InputStream inputStream;
    try {
      Metadata metadata = FileSystems.matchSingleFileSpec(schemasLocation.get());
      ReadableByteChannel channel = FileSystems.open(metadata.resourceId());
      inputStream = Channels.newInputStream(channel);
    } catch (IOException e) {
      throw new IOException("Exception thrown while fetching from configured schemasLocation", e);
    }
    try (InputStream bi = new BufferedInputStream(inputStream);
        InputStream gzi = new GzipCompressorInputStream(bi);
        ArchiveInputStream i = new TarArchiveInputStream(gzi);) {
      ArchiveEntry entry = null;
      while ((entry = i.getNextEntry()) != null) {
        if (!i.canReadEntryData(entry)) {
          LOG.warn("Unable to read tar file entry: " + entry.getName());
          continue;
        }
        String[] components = entry.getName().split("/");
        if (components.length > 2 && "schemas".equals(components[1])) {
          String name = String.join("/", Arrays.copyOfRange(components, 2, components.length));
          if (entry.isDirectory()) {
            tempDirs.add(name);
          } else if (name.endsWith(".schema.json")) {
            byte[] bytes = IOUtils.toByteArray(i);
            JSONObject json = Json.readJSONObject(bytes);
            tempSchemas.put(name, SchemaLoader.load(json));
          }
        }
      }
    }
    schemas = tempSchemas;
    dirs = tempDirs;
  }

  private void ensureSchemasLoaded() {
    if (schemas == null) {
      try {
        loadAllSchemas();
      } catch (IOException e) {
        throw new UncheckedIOException("Unexpected error while loading schemas", e);
      }
    }
  }

}
