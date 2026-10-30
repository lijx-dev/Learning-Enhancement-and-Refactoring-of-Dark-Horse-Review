package com.hmdp.cache;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Opt-in observations for the current thread, without retaining cache keys or entity data. */
public final class CacheReadTrace implements AutoCloseable {
  private static final ThreadLocal<CacheReadTrace> CURRENT = new ThreadLocal<>();
  private final CacheReadTrace previous;
  private final List<Map<String, String>> events = new ArrayList<>();

  private CacheReadTrace() {
    previous = CURRENT.get();
    CURRENT.set(this);
  }

  public static CacheReadTrace open() {
    return new CacheReadTrace();
  }

  public static void record(String layer, String result) {
    CacheReadTrace trace = CURRENT.get();
    if (trace != null && trace.events.size() < 64) {
      Map<String, String> event = new LinkedHashMap<>();
      event.put("layer", layer);
      event.put("result", result);
      trace.events.add(event);
    }
  }

  public List<Map<String, String>> snapshot() {
    List<Map<String, String>> copy = new ArrayList<>();
    for (Map<String, String> event : events) {
      copy.add(new LinkedHashMap<>(event));
    }
    return copy;
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
