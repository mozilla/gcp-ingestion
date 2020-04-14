package com.mozilla.telemetry.util;

import com.google.common.io.Resources;
import org.everit.json.schema.ValidationException;
import org.junit.Test;

public class KeyStoreTest {

  @Test(expected = ValidationException.class)
  public void testMetadataInvalidFormat() {
    String metadataLocation = Resources.getResource("pioneer/metadata-invalid.json").getPath();
    KeyStore store = KeyStore.of(metadataLocation);
    // force the store to load
    store.getKey("*");
  }

  @Test
  public void testNumKeys() {
  }

  @Test
  public void testGetKey() {
  }

}
