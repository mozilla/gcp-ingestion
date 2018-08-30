/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.apache.beam.sdk.testing.{PAssert, TestPipeline}
import org.apache.beam.sdk.transforms.Create
import org.scalatest.{FlatSpec, Matchers}

class SinkTest extends FlatSpec with Matchers {
  "PubsubMessage" must "convert to and from json" in {
    val message = new PubsubMessage("test".getBytes, null)

    val output = TestPipeline.create()
      .apply(Create.of(message))
      .apply(Sink.encodeJson)
      .apply(Sink.decodeJson)
      .get(Sink.decodeJson.mainTag)

    PAssert.that(output).containsInAnyOrder(message)
  }
}
