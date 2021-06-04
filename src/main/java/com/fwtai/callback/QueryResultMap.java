package com.fwtai.callback;

import io.vertx.core.json.JsonObject;

public interface QueryResultMap{

  public void succeed(final JsonObject jsonObject);
  public void failure(final Throwable throwable);
}