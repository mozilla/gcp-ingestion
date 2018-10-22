/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import static org.junit.Assert.assertTrue;

import nl.basjes.parse.useragent.debug.UserAgentAnalyzerTester;
import org.junit.Test;

public class UserAgentsTest {

  @Test
  public void runDocumentationExample() {
    UserAgentAnalyzerTester uaa = new UserAgentAnalyzerTester("UserAgents/*.yaml");
    assertTrue(uaa.runTests(false, true));
  }
}
