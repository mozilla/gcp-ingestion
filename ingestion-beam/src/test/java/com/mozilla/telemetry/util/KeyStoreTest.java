package com.mozilla.telemetry.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.google.common.io.Resources;
import org.everit.json.schema.ValidationException;
import org.junit.Test;

public class KeyStoreTest {

  @Test(expected = IllegalArgumentException.class)
  public void testNullMetadataLocation() {
    KeyStore.of(null, false);
  }

  @Test(expected = ValidationException.class)
  public void testMetadataInvalidFormat() {
    String metadataLocation = Resources.getResource("pioneer/metadata-invalid.json").getPath();
    KeyStore store = KeyStore.of(metadataLocation, false);
    // force the store to load
    store.getKey("*");
  }

  @Test
  public void testNumKeys() {
    String metadataLocation = Resources.getResource("pioneer/metadata-local.json").getPath();
    KeyStore store = KeyStore.of(metadataLocation, false);
    assertEquals(2, store.numLoadedKeys());
  }

  @Test
  public void testGetKey() {
    String metadataLocation = Resources.getResource("pioneer/metadata-local.json").getPath();
    KeyStore store = KeyStore.of(metadataLocation, false);
    assertNotEquals(null, store.getKey("study-foo"));
    assertNotEquals(null, store.getKey("study-bar"));
    assertEquals(null, store.getKey("invalid-document-namespace"));
  }

}
