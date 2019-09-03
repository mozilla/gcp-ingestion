/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.sink.util;

import com.google.pubsub.v1.PubsubMessage;

public abstract class TestWithSinglePubsubTopic extends TestWithPubsubResources {

  protected int numTopics() {
    return 1;
  }

  protected String getSubscription() {
    return getSubscription(0);
  }

  protected String publish(PubsubMessage message) {
    return publish(0, message);
  }
}
