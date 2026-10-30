package com.hmdp.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.alibaba.otter.canal.client.CanalConnector;
import com.hmdp.entity.Shop;
import com.hmdp.service.impl.SeckillServiceImpl;
import com.hmdp.utils.CacheClient;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.util.ReflectionTestUtils;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {"citylens.reliability.scheduling-enabled=false", "hmdp.cdc.enabled=true"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class CacheCdcIntegrationTest {
  @Autowired JdbcTemplate jdbc;
  @Autowired StringRedisTemplate redis;
  @Autowired CacheClient cache;
  @Autowired MemcacheStore shared;
  @Autowired MeterRegistry metrics;
  @Autowired SeckillServiceImpl seckill;
  @Autowired ApplicationContext context;

  @BeforeEach
  void guard() {
    assertEquals("hmdp_optimization_test", jdbc.queryForObject("SELECT DATABASE()", String.class));
    cache.invalidate("cache:shop:900201");
    cache.invalidate("cache:voucher-meta:900202");
    jdbc.update("DELETE FROM tb_voucher WHERE id=900202");
  }

  @Test
  void externalSqlWarmsCurrentValueAndMakesOldMemcacheNamespaceUnreachable() throws Exception {
    await(() -> metrics.get("hmdp.cdc.connected").gauge().value() == 1);
    long id = 900201;
    String old = "cdc-old";
    String fresh = "cdc-new-" + System.nanoTime();

    jdbc.update(
        "INSERT INTO "
            + "tb_shop(id,name,type_id,images,area,address,x,y,"
            + "avg_price,sold,comments,score) VALUES(?, ?,1,'img','test','test',"
            + "1,1,1,1,1,50) ON DUPLICATE KEY UPDATE name=VALUES(name)",
        id,
        old);
    await(() -> old.equals(read(id).getName()));
    String before = version(id);
    jdbc.update(
        "UPDATE tb_shop SET name=? WHERE id=?",
        fresh,
        id); // Direct SQL bypasses service and Outbox.
    await(
        () ->
            !before.equals(version(id))
                && fresh.equals(read(id).getName())
                && shared.get("cache:shop:" + id, version(id)) != null);
    // Warming happens before the batch audit commits; wait for the independent durable
    // checkpoint.
    await(
        () ->
            jdbc.queryForObject(
                    "SELECT COUNT(*) FROM tb_cache_cdc_checkpoint WHERE "
                        + "destination='example' AND binlog_offset>0",
                    Long.class)
                > 0);
    CacheClient peer = new CacheClient(redis, shared);
    try {
      Shop value =
          peer.queryWithMutex(
              "cache:shop:",
              id,
              Shop.class,
              key -> {
                fail("A new instance must hit shared warm data");
                return null;
              },
              20L,
              TimeUnit.MINUTES);
      assertEquals(fresh, value.getName());
    } finally {
      peer.close();
    }
    jdbc.update("DELETE FROM tb_shop WHERE id=?", id);
    await(() -> read(id) == null);
    seckill.refreshMetrics();
    assertTrue(metrics.get("hmdp.seckill.lag").gauge().value() >= 0);
  }

  @Test
  void voucherMoveInvalidatesBothShopAggregates() throws Exception {
    await(() -> metrics.get("hmdp.cdc.connected").gauge().value() == 1);
    long id = 900202;
    jdbc.update(
        "INSERT INTO "
            + "tb_voucher(id,shop_id,title,rules,pay_value,actual_value,type,status) "
            + "VALUES(?,1,'cdc-voucher','rules',0,100,0,1) ON DUPLICATE KEY UPDATE "
            + "shop_id=1",
        id);
    await(() -> redis.opsForValue().get("cache:voucher-meta:" + id) != null);
    String oldOwner = revision("cache:voucher-list:1");
    String newOwner = revision("cache:voucher-list:2");

    jdbc.update("UPDATE tb_voucher SET shop_id=2,title='moved' WHERE id=?", id);
    await(
        () ->
            !oldOwner.equals(revision("cache:voucher-list:1"))
                && !newOwner.equals(revision("cache:voucher-list:2")));
    await(
        () -> {
          String raw = redis.opsForValue().get("cache:voucher-meta:" + id);
          return raw != null && raw.contains("moved");
        });
  }

  @Test
  void clientReconnectResumesAndProcessesSubsequentChange() throws Exception {
    await(() -> metrics.get("hmdp.cdc.connected").gauge().value() == 1);
    CanalCacheConsumer consumer = context.getBean(CanalCacheConsumer.class);
    CanalConnector previous = (CanalConnector) ReflectionTestUtils.getField(consumer, "active");
    assertNotNull(previous);
    previous.disconnect();
    await(
        () -> {
          Object active = ReflectionTestUtils.getField(consumer, "active");
          return active != null
              && active != previous
              && metrics.get("hmdp.cdc.connected").gauge().value() == 1;
        });
    long id = 900203;
    String name = "after-reconnect-" + System.nanoTime();
    jdbc.update(
        "INSERT INTO "
            + "tb_shop(id,name,type_id,images,area,address,x,y,"
            + "avg_price,sold,comments,score) VALUES(?, ?,1,'img','test','test',"
            + "1,1,1,1,1,50) ON DUPLICATE KEY UPDATE name=VALUES(name)",
        id,
        name);
    await(
        () -> {
          CacheSnapshot value = shared.get("cache:shop:" + id, version(id));
          return value != null && value.json.contains(name);
        });
    jdbc.update("DELETE FROM tb_shop WHERE id=?", id);
  }

  private Shop read(long id) {
    return cache.queryWithMutex(
        "cache:shop:",
        id,
        Shop.class,
        key ->
            jdbc
                .query(
                    "SELECT id,name FROM tb_shop WHERE id=?",
                    (rs, row) -> new Shop().setId(rs.getLong(1)).setName(rs.getString(2)),
                    key)
                .stream()
                .findFirst()
                .orElse(null),
        20L,
        TimeUnit.MINUTES);
  }

  private String version(long id) {
    return revision("cache:shop:" + id);
  }

  private String revision(String key) {
    String raw = redis.opsForValue().get(key + ":version");
    return raw == null ? "0" : raw;
  }

  private static void await(BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(100);
    }
    assertTrue(condition.getAsBoolean(), "Real Canal CDC did not converge within 30 seconds");
  }
}
