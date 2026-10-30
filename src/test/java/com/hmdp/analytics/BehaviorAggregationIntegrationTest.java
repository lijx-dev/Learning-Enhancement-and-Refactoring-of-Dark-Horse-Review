package com.hmdp.analytics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hmdp.service.impl.BehaviorAggregationServiceImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootTest(
    properties = {"citylens.reliability.scheduling-enabled=false", "hmdp.analytics.enabled=false"})
class BehaviorAggregationIntegrationTest {
  @Autowired JdbcTemplate jdbc;
  @Autowired BehaviorAggregationServiceImpl aggregation;

  @BeforeEach
  void isolate() {
    assertEquals("hmdp_optimization_test", jdbc.queryForObject("SELECT DATABASE()", String.class));
    jdbc.update("DELETE FROM tb_behavior_event");
    jdbc.update("DELETE FROM tb_shop_heat_bucket");
    jdbc.update("DELETE FROM tb_shop_visit_user");
  }

  private static BehaviorEvent event(String id, Long user, String action, long time) {
    return new BehaviorEvent(id, 1, user, action, time);
  }

  @Test
  void windowUvDeduplicatesAcrossMinutesAndAnonymousVisitsOnlyAddPv() {
    long now = System.currentTimeMillis() / 60000 * 60000;
    aggregation.accept(event("visit-a", 100L, "VISIT", now));
    aggregation.accept(event("visit-b", 100L, "VISIT", now - 60000));
    aggregation.accept(event("visit-c", 101L, "VISIT", now));
    aggregation.accept(event("anonymous", null, "VISIT", now));
    aggregation.accept(event("order", 100L, "ORDER", now));
    aggregation.accept(event("payment", 100L, "PAID", now));
    Map<String, Object> rank = aggregation.ranking(5, 10).get(0);
    assertEquals(4, ((Number) rank.get("pv")).intValue());
    assertEquals(2, ((Number) rank.get("logged_uv")).intValue());
    assertEquals(1, ((Number) rank.get("orders")).intValue());
    assertEquals(1, ((Number) rank.get("mock_payments")).intValue());
    assertEquals(23, ((Number) rank.get("heat")).intValue());
  }

  @Test
  void repeatedAndOutOfOrderDeliveryCannotDoubleCount() throws Exception {
    long now = System.currentTimeMillis();
    BehaviorEvent message = event("same-id", 100L, "VISIT", now);
    ExecutorService pool = Executors.newFixedThreadPool(6);
    try {
      List<Future<?>> results = new ArrayList<>();
      for (int i = 0; i < 18; i++) {
        results.add(pool.submit(() -> aggregation.accept(message)));
      }
      for (Future<?> result : results) {
        result.get(10, TimeUnit.SECONDS);
      }
      aggregation.accept(event("older", 100L, "VISIT", now - 60000));
      aggregation.accept(message);
      assertEquals(
          2L, jdbc.queryForObject("SELECT SUM(visits) FROM tb_shop_heat_bucket", Long.class));
      assertEquals(1, ((Number) aggregation.ranking(5, 10).get(0).get("logged_uv")).intValue());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void oldEventsAreRetainedButExcludedFromCurrentShortWindow() {
    aggregation.accept(event("old", 100L, "VISIT", System.currentTimeMillis() - 10 * 60000));
    assertTrue(aggregation.ranking(5, 10).isEmpty());
    assertEquals(1, aggregation.ranking(60, 10).size());
  }

  @Test
  void conflictingIdentityIsRejectedWithoutChangingCounters() {
    long now = System.currentTimeMillis();
    aggregation.accept(event("conflict", 100L, "VISIT", now));
    assertThrows(
        IllegalArgumentException.class,
        () -> aggregation.accept(event("conflict", 101L, "PAID", now)));
    assertEquals(
        1L, jdbc.queryForObject("SELECT SUM(visits) FROM tb_shop_heat_bucket", Long.class));
    assertEquals(
        0L, jdbc.queryForObject("SELECT SUM(payments) FROM tb_shop_heat_bucket", Long.class));
  }

  @Test
  void validatesSchemaFutureTimestampAndWindowBounds() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            aggregation.accept(
                event("future", 100L, "VISIT", System.currentTimeMillis() + 120000)));
    assertThrows(
        IllegalArgumentException.class,
        () -> aggregation.accept(event("action", 100L, "UNKNOWN", System.currentTimeMillis())));
    assertThrows(IllegalArgumentException.class, () -> aggregation.ranking(30, 10));
  }
}
