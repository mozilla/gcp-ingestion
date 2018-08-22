/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class LandfillTest extends FlatSpec with Matchers {
  "pubsubMessages" must "convert to and from json" in {
    val emptyMessage = new PubsubMessage(Array[Byte](), Map[String,String]().asJava)
    // TODO: write this test
  }
}
