package com.hmdp.utils;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.hmdp.cache.CacheReadTrace;
import com.hmdp.cache.CacheSnapshot;
import com.hmdp.cache.MemcacheStore;
import com.hmdp.cache.RequestCacheScope;
import com.hmdp.reliability.RedisScripts;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CacheClient {
  private static final long REBUILD_WAIT_SECONDS = 2;
  private static final long REBUILD_LOCK_SECONDS = 10;
  private static final int RETRY_BASE_MILLIS = 20;
  private static final int RETRY_JITTER_MILLIS = 20;
  private final StringRedisTemplate redis;
  private final MemcacheStore shared;
  private final AtomicLong generation = new AtomicLong();
  private final LoadingCache<LoadRequest, CacheSnapshot> local =
      Caffeine.newBuilder()
          .maximumSize(10000)
          .expireAfterWrite(5, TimeUnit.SECONDS)
          .recordStats()
          .build(this::loadLayers);
  private final ThreadPoolExecutor rebuild =
      new ThreadPoolExecutor(
          2,
          4,
          30,
          TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(100),
          r -> {
            Thread t = new Thread(r, "cache-rebuild");
            t.setDaemon(true);
            return t;
          },
          new ThreadPoolExecutor.AbortPolicy());
  private static final DefaultRedisScript<Long> UNLOCK = RedisScripts.number("unlock.lua");
  private static final DefaultRedisScript<Long> FILL = RedisScripts.number("cache-fill.lua");
  private static final DefaultRedisScript<Long> INVALIDATE =
      RedisScripts.number("cache-invalidate.lua");

  @Value("${hmdp.cache.enabled:true}")
  private boolean enabled = true;

  private MeterRegistry metrics;

  public CacheClient(StringRedisTemplate redis) {
    this(redis, new MemcacheStore());
  }

  @Autowired
  public CacheClient(StringRedisTemplate redis, MemcacheStore shared) {
    this.redis = redis;
    this.shared = shared;
  }

  @Autowired
  public void bindMetrics(MeterRegistry registry) {
    metrics = registry;
    CaffeineCacheMetrics.monitor(registry, local, "business_details");
  }

  @PreDestroy
  public void close() {
    rebuild.shutdownNow();
  }

  /**
   * 递增整个本地缓存命名空间的世代，后续请求改用新世代查询。
   *
   * <p>旧加载即使晚到，也只能写入旧世代；这是逻辑失效，不是按单个 key 立即删除全部对象。
   */
  public void advanceLocalCacheGeneration() {
    generation.incrementAndGet();
  }

  public CacheStats stats() {
    return local.stats();
  }

  public void invalidate(String key) {
    advanceLocalCacheGeneration();
    redis.execute(INVALIDATE, Arrays.asList(key, key + ":version"));
  }

  public void invalidatePrefix(String prefix) {
    advanceLocalCacheGeneration();
    redis.execute(
        (RedisCallback<Object>)
            connection -> {
              try (Cursor<byte[]> cursor =
                  connection.scan(
                      ScanOptions.scanOptions().match(prefix + "*").count(200).build())) {
                while (cursor.hasNext()) {
                  String key = new String(cursor.next(), StandardCharsets.UTF_8);
                  if (key.endsWith(":version")) {
                    key = key.substring(0, key.length() - 8);
                  }
                  // Use another pooled connection; mutation during SCAN may
                  // repeat a key, which is harmless.
                  invalidate(key);
                }
              } catch (RuntimeException failure) {
                throw new IllegalStateException("缓存命名空间失效失败", failure);
              }
              return null;
            });
  }

  private String queryCacheVersion(String key) {
    String value = redis.opsForValue().get(key + ":version");
    return value == null ? "0" : value;
  }

  private void recordCacheReadOutcome(String layer, String result) {
    CacheReadTrace.record(layer, result);
    if (metrics != null) {
      metrics.counter("hmdp.cache.requests", "layer", layer, "result", result).increment();
    }
  }

  /**
   * 把传入时长统一换算为毫秒，并增加不足基准 TTL 10% 的随机抖动。
   *
   * <p>分散同批缓存的过期时间，减少集中回源；返回值始终以毫秒为单位。
   */
  private long calculateTtlMillisWithJitter(Long duration, TimeUnit durationUnit) {
    long baseTtlMillis = durationUnit.toMillis(duration);
    return baseTtlMillis + ThreadLocalRandom.current().nextLong(Math.max(1, baseTtlMillis / 10));
  }

  public void set(String key, Object value, Long duration, TimeUnit durationUnit) {
    invalidate(key);
    redis
        .opsForValue()
        .set(
            key,
            JSONUtil.toJsonStr(value),
            calculateTtlMillisWithJitter(duration, durationUnit),
            TimeUnit.MILLISECONDS);
  }

  public void setWithLogicalExpire(String key, Object value, Long duration, TimeUnit durationUnit) {
    RedisData data = new RedisData();
    data.setData(value);
    data.setExpireTime(LocalDateTime.now().plusSeconds(durationUnit.toSeconds(duration)));
    invalidate(key);
    redis.opsForValue().set(key, JSONUtil.toJsonStr(data));
  }

  private <R> R decode(String json, Class<R> type) {
    JSONObject object = JSONUtil.parseObj(json);
    if (object.containsKey("expireTime") && object.containsKey("data")) {
      return JSONUtil.toBean(object.getJSONObject("data"), type);
    }
    return JSONUtil.toBean(object, type);
  }

  private boolean isUsableCachePayload(String json) {
    if (json.isEmpty()) {
      return true;
    }
    try {
      JSONObject object = JSONUtil.parseObj(json);
      return !object.containsKey("expireTime")
          || LocalDateTime.parse(object.getStr("expireTime")).isAfter(LocalDateTime.now());
    } catch (RuntimeException corrupt) {
      return false;
    }
  }

  private CacheSnapshot readRedisSnapshot(String key) {
    String json = redis.opsForValue().get(key);
    if (json == null) {
      recordCacheReadOutcome("redis", "miss");
      return null;
    }
    if (!isUsableCachePayload(json)) {
      invalidate(key);
      return null;
    }
    Long ttlMillis = redis.getExpire(key, TimeUnit.MILLISECONDS);
    if (ttlMillis != null && ttlMillis == -2) {
      return null;
    }
    recordCacheReadOutcome("redis", "hit");
    return new CacheSnapshot(
        json,
        System.currentTimeMillis() + (ttlMillis == null || ttlMillis < 0 ? 30000 : ttlMillis),
        true);
  }

  private CacheSnapshot loadLayers(LoadRequest request) {
    String key = request.key;
    String revision = queryCacheVersion(key);
    CacheSnapshot cached = shared.get(key, revision);
    if (cached != null && isUsableCachePayload(cached.json)) {
      recordCacheReadOutcome("memcache", "hit");
      return cached;
    }
    recordCacheReadOutcome("memcache", "miss");
    cached = readRedisSnapshot(key);
    if (cached != null) {
      shared.put(key, revision, cached);
      return cached;
    }
    return awaitOrRebuild(request);
  }

  private CacheSnapshot awaitOrRebuild(LoadRequest request) {
    String key = request.key;
    String lockKey = "lock:cache:" + key;
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(REBUILD_WAIT_SECONDS);
    do {
      String owner = UUID.randomUUID().toString();
      if (Boolean.TRUE.equals(
          redis
              .opsForValue()
              .setIfAbsent(lockKey, owner, REBUILD_LOCK_SECONDS, TimeUnit.SECONDS))) {
        return rebuildUnderLock(request, lockKey, owner);
      }
      String polledVersion = queryCacheVersion(key);
      CacheSnapshot cached = readRedisSnapshot(key);
      if (cached != null) {
        shared.put(key, polledVersion, cached);
        return cached;
      }
      pauseBeforeRetry();
    } while (System.nanoTime() < deadline);
    throw new IllegalStateException("热点数据正在重建，请稍后重试");
  }

  private CacheSnapshot rebuildUnderLock(LoadRequest request, String lockKey, String owner) {
    String key = request.key;
    try {
      String revision = queryCacheVersion(key);
      CacheSnapshot cached = readRedisSnapshot(key);
      if (cached != null) {
        shared.put(key, revision, cached);
        return cached;
      }
      revision = queryCacheVersion(key);
      recordCacheReadOutcome("mysql", "load");
      String json = request.source.get();
      long ttlMillis =
          json.isEmpty()
              ? calculateTtlMillisWithJitter(RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES)
              : request.ttlMillis;
      // 同时校验加载前版本与锁 owner；旧加载可返回给本次请求，但不能污染后续缓存。
      Long written =
          redis.execute(
              FILL,
              Arrays.asList(key, lockKey, key + ":version"),
              json,
              String.valueOf(ttlMillis),
              revision,
              owner);
      CacheSnapshot loaded =
          new CacheSnapshot(
              json, System.currentTimeMillis() + ttlMillis, Long.valueOf(1).equals(written));
      if (loaded.cacheable) {
        shared.put(key, revision, loaded);
      } else {
        recordCacheReadOutcome("fill", "fenced");
      }
      return loaded;
    } finally {
      redis.execute(UNLOCK, Collections.singletonList(lockKey), owner);
    }
  }

  private void pauseBeforeRetry() {
    try {
      Thread.sleep(RETRY_BASE_MILLIS + ThreadLocalRandom.current().nextInt(RETRY_JITTER_MILLIS));
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("缓存查询已中断", interrupted);
    }
  }

  /**
   * 保留原教学入口；当前委托给统一的多层缓存和互斥回源实现。
   *
   * <p>方法名源于早期缓存穿透示例，实际读取策略请以 queryWithMutex 为准。
   */
  public <R, ID> R queryWithPassThrough(
      String prefix,
      ID id,
      Class<R> type,
      Function<ID, R> source,
      Long duration,
      TimeUnit durationUnit) {
    return queryWithMutex(prefix, id, type, source, duration, durationUnit);
  }

  public <R, ID> R queryWithMutex(
      String prefix,
      ID id,
      Class<R> type,
      Function<ID, R> source,
      Long duration,
      TimeUnit durationUnit) {
    if (!enabled) {
      recordCacheReadOutcome("mysql", "load");
      return source.apply(id);
    }
    String key = prefix + id;
    long cacheGeneration = generation.get();
    String scopeKey = key + "@" + cacheGeneration;
    CacheSnapshot snapshot = RequestCacheScope.get(scopeKey);
    if (snapshot != null && snapshot.fresh()) {
      recordCacheReadOutcome("scope", "hit");
    } else {
      recordCacheReadOutcome("scope", "miss");
      LoadRequest request =
          new LoadRequest(
              key,
              cacheGeneration,
              calculateTtlMillisWithJitter(duration, durationUnit),
              () -> {
                R value = source.apply(id);
                return value == null ? "" : JSONUtil.toJsonStr(value);
              });
      snapshot = local.getIfPresent(request);
      if (snapshot != null && snapshot.fresh()) {
        recordCacheReadOutcome("jvm", "hit");
      } else {
        if (snapshot != null) {
          local.invalidate(request);
        }
        recordCacheReadOutcome("jvm", "miss");
        snapshot = local.get(request);
      }
      if (!snapshot.cacheable) {
        local.invalidate(request);
      } else if (cacheGeneration == generation.get()) {
        RequestCacheScope.put(scopeKey, snapshot);
      }
    }
    return snapshot.json.isEmpty() ? null : decode(snapshot.json, type);
  }

  private static final class LoadRequest {
    final String key;
    final long cacheGeneration;
    final long ttlMillis;

    final Supplier<String> source;

    LoadRequest(String key, long cacheGeneration, long ttlMillis, Supplier<String> source) {
      this.key = key;
      this.cacheGeneration = cacheGeneration;
      this.ttlMillis = ttlMillis;
      this.source = source;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof LoadRequest
          && key.equals(((LoadRequest) other).key)
          && cacheGeneration == ((LoadRequest) other).cacheGeneration;
    }

    @Override
    public int hashCode() {
      return 31 * key.hashCode() + Long.hashCode(cacheGeneration);
    }
  }

  public <R, ID> R queryWithLogicalExpire(
      String prefix,
      ID id,
      Class<R> type,
      Function<ID, R> source,
      Long duration,
      TimeUnit durationUnit) {
    String key = prefix + id;
    String json = redis.opsForValue().get(key);

    if (json == null || json.isEmpty()) {
      return queryWithMutex(prefix, id, type, source, duration, durationUnit);
    }
    JSONObject object;
    try {
      object = JSONUtil.parseObj(json);
    } catch (RuntimeException corrupt) {
      invalidate(key);
      return queryWithMutex(prefix, id, type, source, duration, durationUnit);
    }
    if (!object.containsKey("expireTime")) {
      return decode(json, type);
    }
    R value = decode(json, type);
    if (LocalDateTime.parse(object.getStr("expireTime")).isAfter(LocalDateTime.now())) {
      return value;
    }
    String lockKey = "lock:cache:" + key;
    String owner = UUID.randomUUID().toString();

    if (Boolean.TRUE.equals(
        redis.opsForValue().setIfAbsent(lockKey, owner, 10, TimeUnit.SECONDS))) {
      String revision = queryCacheVersion(key);
      try {
        rebuild.execute(
            () -> {
              try {
                R fresh = source.apply(id);
                RedisData data = new RedisData();
                data.setData(fresh);
                data.setExpireTime(
                    LocalDateTime.now().plusSeconds(durationUnit.toSeconds(duration)));
                redis.execute(
                    FILL,
                    Arrays.asList(key, lockKey, key + ":version"),
                    fresh == null ? "" : JSONUtil.toJsonStr(data),
                    String.valueOf(calculateTtlMillisWithJitter(duration * 2, durationUnit)),
                    revision,
                    owner);
                advanceLocalCacheGeneration();
              } catch (RuntimeException e) {
                log.warn("缓存重建失败: {}", key, e);
              } finally {
                redis.execute(UNLOCK, Collections.singletonList(lockKey), owner);
              }
            });
      } catch (RejectedExecutionException full) {
        redis.execute(UNLOCK, Collections.singletonList(lockKey), owner);
      }
    }
    return value;
  }
}
