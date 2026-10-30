package com.hmdp.cache;

import cn.hutool.crypto.digest.DigestUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.PreDestroy;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class MemcacheStore {
  @Value("${hmdp.memcache.enabled:false}")
  private boolean enabled;

  @Value("${hmdp.memcache.host:127.0.0.1}")
  private String host;

  @Value("${hmdp.memcache.port:11211}")
  private int port;

  @Value("${hmdp.memcache.timeout-ms:100}")
  private long timeoutMillis;

  @Autowired(required = false)
  private MeterRegistry metrics;

  private volatile MemcachedClient client;
  private final AtomicLong retryAfter = new AtomicLong();

  public static String key(String key, String version) {
    return "hmdp:" + DigestUtil.sha256Hex(key) + ":" + version;
  }

  private MemcachedClient connection() throws IOException {
    if (client == null) {
      synchronized (this) {
        if (client == null) {
          client =
              new MemcachedClient(
                  new ConnectionFactoryBuilder()
                      .setOpTimeout(timeoutMillis)
                      .setFailureMode(FailureMode.Cancel)
                      .setOpQueueMaxBlockTime(timeoutMillis)
                      .setDaemon(true)
                      .build(),
                  Collections.singletonList(new InetSocketAddress(host, port)));
        }
      }
    }
    return client;
  }

  private void count(String operation, String result) {
    if (metrics != null) {
      metrics
          .counter("hmdp.memcache.operations", "operation", operation, "result", result)
          .increment();
    }
  }

  private boolean available() {
    return enabled && System.currentTimeMillis() >= retryAfter.get();
  }

  public CacheSnapshot get(String key, String version) {
    if (!available()) {
      return null;
    }
    Future<Object> future = null;
    try {
      future = connection().asyncGet(key(key, version));
      Object raw = future.get(timeoutMillis, TimeUnit.MILLISECONDS);
      if (raw == null) {
        count("get", "miss");
        return null;
      }
      JSONObject value = JSONUtil.parseObj(raw.toString());
      CacheSnapshot snapshot =
          new CacheSnapshot(value.getStr("json"), value.getLong("expiresAt", 0L), true);
      if (snapshot.json == null || !snapshot.fresh()) {
        count("get", "expired");
        return null;
      }
      count("get", "hit");
      return snapshot;
    } catch (Exception e) {
      if (future != null) {
        future.cancel(false);
      }
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      retryAfter.set(System.currentTimeMillis() + 1000);
      count("get", "error");
      return null;
    }
  }

  public boolean put(String key, String version, CacheSnapshot snapshot) {
    if (!available() || !snapshot.cacheable || !snapshot.fresh()) {
      return false;
    }
    Future<Boolean> future = null;
    try {
      // Immutable version-qualified keys make an old in-flight write unreachable after
      // invalidation.
      long remaining = Math.min(30000, snapshot.expiresAt - System.currentTimeMillis());
      if (remaining <= 0) {
        return false;
      }
      String envelope =
          JSONUtil.createObj()
              .set("json", snapshot.json)
              .set("expiresAt", Math.min(snapshot.expiresAt, System.currentTimeMillis() + 30000))
              .toString();
      future =
          connection()
              .set(key(key, version), (int) Math.max(1, (remaining + 999) / 1000), envelope);
      boolean stored = Boolean.TRUE.equals(future.get(timeoutMillis, TimeUnit.MILLISECONDS));
      count("put", stored ? "success" : "error");
      return stored;
    } catch (Exception e) {
      if (future != null) {
        future.cancel(false);
      }
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      retryAfter.set(System.currentTimeMillis() + 1000);
      count("put", "error");
      return false;
    }
  }

  @PreDestroy
  public void close() {
    MemcachedClient current = client;
    if (current != null) {
      current.shutdown();
    }
  }
}
