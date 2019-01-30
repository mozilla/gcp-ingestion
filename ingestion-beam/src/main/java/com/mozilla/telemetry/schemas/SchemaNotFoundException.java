/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.schemas;

public class SchemaNotFoundException extends Exception {

  public SchemaNotFoundException(String message) {
    super(message);
  }

  public static SchemaNotFoundException forName(String name) {
    return new SchemaNotFoundException("No schema with name: " + name);
  }
}
