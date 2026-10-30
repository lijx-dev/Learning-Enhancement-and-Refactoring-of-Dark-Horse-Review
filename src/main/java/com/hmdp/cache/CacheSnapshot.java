package com.hmdp.cache;

public final class CacheSnapshot {
  public final String json;
  public final long expiresAt;
  public final boolean cacheable;

  public CacheSnapshot(String json, long expiresAt, boolean cacheable) {
    this.json = json;
    this.expiresAt = expiresAt;
    this.cacheable = cacheable;
  }

  public boolean fresh() {
    return expiresAt > System.currentTimeMillis();
  }
}
