package com.hmdp.cache;

import java.util.HashMap;
import java.util.Map;

/**
 * Never inherit request data into consumer/background threads. Values are immutable JSON snapshots.
 */
public final class RequestCacheScope implements AutoCloseable {
  private static final ThreadLocal<Map<String, CacheSnapshot>> CURRENT = new ThreadLocal<>();
  private final Map<String, CacheSnapshot> previous;

  private RequestCacheScope() {
    previous = CURRENT.get();
    CURRENT.set(new HashMap<>());
  }

  public static RequestCacheScope open() {
    return new RequestCacheScope();
  }

  public static CacheSnapshot get(String key) {
    Map<String, CacheSnapshot> map = CURRENT.get();
    return map == null ? null : map.get(key);
  }

  public static void put(String key, CacheSnapshot value) {
    Map<String, CacheSnapshot> map = CURRENT.get();
    if (map != null && map.size() < 256) {
      map.put(key, value);
    }
  }

  public static boolean active() {
    return CURRENT.get() != null;
  }

  @Override
  public void close() {
    if (previous == null) {
      CURRENT.remove();
    } else {
      CURRENT.set(previous);
    }
  }
}
