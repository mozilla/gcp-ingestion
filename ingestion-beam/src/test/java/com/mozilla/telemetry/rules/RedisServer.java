package com.mozilla.telemetry.rules;

import org.junit.rules.ExternalResource;

public class RedisServer extends ExternalResource {

  public final int port = new redis.embedded.ports.EphemeralPortProvider().next();
  public final String uri = "redis://localhost:" + port;
  private final redis.embedded.RedisServer server = redis.embedded.RedisServer.builder().port(port)
      .setting("bind 127.0.0.1").build();

  @Override
  protected void before() throws Throwable {
    super.before();
    server.start();
  }

  @Override
  protected void after() {
    super.after();
    server.stop();
  }
}
