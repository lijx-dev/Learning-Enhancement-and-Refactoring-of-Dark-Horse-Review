package com.hmdp.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.Voucher;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.CacheClient;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {"citylens.reliability.scheduling-enabled=false", "hmdp.cdc.enabled=false"})
class CacheLayersIntegrationTest {
  @Autowired StringRedisTemplate redis;
  @Autowired JdbcTemplate jdbc;
  @Autowired MemcacheStore shared;
  @Autowired CacheClient serviceCache;
  @Autowired TestRestTemplate http;
  @Autowired IVoucherService vouchers;
  private CacheClient cache;
  private SimpleMeterRegistry registry;
  private String prefix;

  @BeforeEach
  void init() throws InterruptedException {
    assertEquals("hmdp_optimization_test", jdbc.queryForObject("SELECT DATABASE()", String.class));
    prefix = "test:layers:" + UUID.randomUUID() + ":";
    cache = new CacheClient(redis, shared);
    registry = new SimpleMeterRegistry();
    cache.bindMetrics(registry);
    // A previous context may have just closed its client. Wait for this client's
    // real connection before asserting short TTLs or cross-client cache hits.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!shared.put(
        prefix + "ready", "0", new CacheSnapshot("{}", System.currentTimeMillis() + 10000, true))) {
      assertTrue(System.nanoTime() < deadline, "隔离 Memcache 未在 5 秒内就绪");
      Thread.sleep(100);
    }
  }

  @AfterEach
  void close() {
    cache.close();
    registry.close();
  }

  private Shop shop(String name) {
    Shop s = new Shop();
    s.setId(1L);
    s.setName(name);
    return s;
  }

  private Shop read(CacheClient client, AtomicInteger loads) {
    return client.queryWithMutex(
        prefix,
        1L,
        Shop.class,
        id -> {
          loads.incrementAndGet();
          return shop("database");
        },
        1L,
        TimeUnit.MINUTES);
  }

  @Test
  void scopeLocalSharedAndRedisServeImmutableCopies() {
    AtomicInteger loads = new AtomicInteger();
    try (RequestCacheScope scope = RequestCacheScope.open()) {
      read(cache, loads).setName("mutated");
      assertEquals("database", read(cache, loads).getName());
      assertEquals(
          1,
          registry
              .get("hmdp.cache.requests")
              .tags("layer", "scope", "result", "hit")
              .counter()
              .count());
    }
    assertEquals("database", read(cache, loads).getName());
    assertEquals(
        1,
        registry
            .get("hmdp.cache.requests")
            .tags("layer", "jvm", "result", "hit")
            .counter()
            .count());
    CacheClient peer = new CacheClient(redis, shared);
    SimpleMeterRegistry peerMetrics = new SimpleMeterRegistry();
    peer.bindMetrics(peerMetrics);
    try {
      assertEquals("database", read(peer, loads).getName());
      assertEquals(
          1,
          peerMetrics
              .get("hmdp.cache.requests")
              .tags("layer", "memcache", "result", "hit")
              .counter()
              .count());
    } finally {
      peer.close();
      peerMetrics.close();
    }
    CacheClient redisOnly = new CacheClient(redis);
    try {
      assertEquals("database", read(redisOnly, loads).getName());
      assertEquals(1, loads.get());
    } finally {
      redisOnly.close();
    }
  }

  @Test
  void concurrentLoadingCoalescesAndInvalidatedFlightCannotPoisonNewGeneration() throws Exception {
    AtomicInteger loads = new AtomicInteger();
    ExecutorService pool = Executors.newFixedThreadPool(8);
    CountDownLatch began = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);

    try {
      List<Future<Shop>> reads = new ArrayList<>();
      for (int i = 0; i < 16; i++) {
        reads.add(
            pool.submit(
                () ->
                    cache.queryWithMutex(
                        prefix,
                        1L,
                        Shop.class,
                        id -> {
                          loads.incrementAndGet();
                          try {
                            Thread.sleep(30);
                          } catch (InterruptedException e) {
                            throw new IllegalStateException(e);
                          }
                          return shop("old");
                        },
                        1L,
                        TimeUnit.MINUTES)));
      }
      for (Future<Shop> f : reads) {
        assertEquals("old", f.get(5, TimeUnit.SECONDS).getName());
      }
      assertEquals(1, loads.get());
      cache.invalidate(prefix + 1);
      Future<Shop> old =
          pool.submit(
              () ->
                  cache.queryWithMutex(
                      prefix,
                      1L,
                      Shop.class,
                      id -> {
                        began.countDown();
                        try {
                          finish.await(5, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                          throw new IllegalStateException(e);
                        }
                        return shop("old");
                      },
                      1L,
                      TimeUnit.MINUTES));
      assertTrue(began.await(3, TimeUnit.SECONDS));
      cache.invalidate(prefix + 1);
      finish.countDown();
      old.get(5, TimeUnit.SECONDS);
      assertNull(redis.opsForValue().get(prefix + 1));
      assertNull(shared.get(prefix + 1, redis.opsForValue().get(prefix + "1:version")));
      assertEquals("database", read(cache, loads).getName());
    } finally {
      finish.countDown();
      pool.shutdownNow();
    }
  }

  @Test
  void versionNamespaceRejectsOldSharedValueAndTtlDoesNotExtendSourceLifetime() throws Exception {
    String key = prefix + 1;
    String revision = "0";
    assertTrue(
        shared.put(
            key,
            revision,
            new CacheSnapshot(
                "{\"id\":1,\"name\":\"stale\"}", System.currentTimeMillis() + 200, true)));
    Thread.sleep(260);
    assertNull(shared.get(key, revision));
    assertTrue(
        shared.put(
            key,
            revision,
            new CacheSnapshot("{\"id\":1,\"name\":\"stale\"}", Long.MAX_VALUE, true)));
    cache.invalidate(key);
    AtomicInteger loads = new AtomicInteger();
    assertEquals("database", read(cache, loads).getName());
    assertEquals(1, loads.get());
  }

  @Test
  void ddlNamespaceScanFencesSharedEntriesAndLocalSnapshots() {
    AtomicInteger loads = new AtomicInteger();
    read(cache, loads);
    String before = redis.opsForValue().get(prefix + "1:version");
    cache.invalidatePrefix(prefix);
    assertNotEquals(before, redis.opsForValue().get(prefix + "1:version"));
    assertNull(redis.opsForValue().get(prefix + 1));
    assertEquals("database", read(cache, loads).getName());
    assertEquals(2, loads.get());
  }

  @Test
  void unavailableMemcacheFallsBackWithinBoundAndKeepsRedisData() {
    MemcacheStore broken = new MemcacheStore();
    ReflectionTestUtils.setField(broken, "enabled", true);
    ReflectionTestUtils.setField(broken, "host", "127.0.0.1");
    ReflectionTestUtils.setField(broken, "port", 1);
    ReflectionTestUtils.setField(broken, "timeoutMillis", 40L);
    ReflectionTestUtils.setField(broken, "metrics", registry);
    CacheClient fallback = new CacheClient(redis, broken);
    redis.opsForValue().set(prefix + 1, "{\"id\":1,\"name\":\"redis\"}", 1, TimeUnit.MINUTES);
    try {
      long began = System.nanoTime();
      assertEquals("redis", read(fallback, new AtomicInteger()).getName());
      assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - began) < 2000);
      assertEquals(
          1,
          registry
              .get("hmdp.memcache.operations")
              .tags("operation", "get", "result", "error")
              .counter()
              .count());
      assertNotNull(redis.opsForValue().get(prefix + 1));
    } finally {
      fallback.close();
      broken.close();
    }
  }

  @Test
  void realActuatorHandlerRequiresDedicatedCredentialOrAdminLogin() {
    assertEquals(401, http.getForEntity("/actuator/prometheus", String.class).getStatusCodeValue());
    HttpHeaders headers = new HttpHeaders();
    headers.set("Authorization", "Bearer wrong");
    assertEquals(
        401,
        http.exchange(
                "/actuator/prometheus", HttpMethod.GET, new HttpEntity<>(headers), String.class)
            .getStatusCodeValue());
    headers.set("Authorization", "Bearer hmdp_dev_metrics");
    assertEquals(
        200,
        http.exchange(
                "/actuator/prometheus", HttpMethod.GET, new HttpEntity<>(headers), String.class)
            .getStatusCodeValue());
    assertEquals(
        401,
        http.exchange("/ops/seckill/tasks", HttpMethod.GET, new HttpEntity<>(headers), String.class)
            .getStatusCodeValue());
    redis.opsForHash().put("login:token:test-monitor-admin", "id", "1010");
    redis.expire("login:token:test-monitor-admin", 1, TimeUnit.MINUTES);
    try {
      headers.set("Authorization", "test-monitor-admin");
      assertEquals(
          200,
          http.exchange(
                  "/actuator/prometheus", HttpMethod.GET, new HttpEntity<>(headers), String.class)
              .getStatusCodeValue());
    } finally {
      redis.delete("login:token:test-monitor-admin");
    }
    assertEquals(200, http.getForEntity("/actuator/health", String.class).getStatusCodeValue());
  }

  @Test
  void voucherMetadataCachesButStockAndWindowRemainLive() {
    long id = 900111;
    jdbc.update(
        "INSERT INTO "
            + "tb_voucher(id,shop_id,title,rules,pay_value,actual_value,type,status) "
            + "VALUES(?,1,'metadata','rules',0,100,1,1) ON DUPLICATE KEY UPDATE "
            + "title='metadata',type=1,status=1",
        id);
    jdbc.update(
        "INSERT INTO tb_seckill_voucher(voucher_id,stock,begin_time,end_time) "
            + "VALUES(?,1,TIMESTAMPADD(HOUR,-1,NOW()),TIMESTAMPADD(DAY,1,NOW())) ON "
            + "DUPLICATE KEY UPDATE "
            + "stock=1,begin_time=VALUES(begin_time),end_time=VALUES(end_time)",
        id);
    serviceCache.invalidate("cache:voucher-list:1");
    serviceCache.invalidate("cache:voucher-meta:" + id);
    assertTrue(find(vouchers.queryVoucherOfShop(1L), id).getOrderable());
    jdbc.update("UPDATE tb_seckill_voucher SET stock=0 WHERE voucher_id=?", id);
    Voucher response = find(vouchers.queryVoucherOfShop(1L), id);
    assertFalse(response.getOrderable());
    assertEquals(0, response.getStock());
    jdbc.update(
        "UPDATE tb_seckill_voucher SET "
            + "stock=5,begin_time=TIMESTAMPADD(DAY,1,NOW()) WHERE voucher_id=?",
        id);
    assertFalse(find(vouchers.queryVoucherOfShop(1L), id).getOrderable());
  }

  @SuppressWarnings("unchecked")
  private Voucher find(Result response, long id) {
    return ((List<Voucher>) response.getData())
        .stream().filter(v -> v.getId() == id).findFirst().get();
  }
}
