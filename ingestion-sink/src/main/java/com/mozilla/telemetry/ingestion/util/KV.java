/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.util;

/**
 * Container class for a key-value pair
 */
public class KV<KeyType, ValueType> {

  public KeyType key;
  public ValueType value;

  public KV(KeyType key, ValueType value) {
    this.key = key;
    this.value = value;
  }
}
