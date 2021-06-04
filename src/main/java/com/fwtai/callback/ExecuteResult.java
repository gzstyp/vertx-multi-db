package com.fwtai.callback;

public interface ExecuteResult{

  public void success(final int rows);
  public void failure(final Throwable throwable);
}