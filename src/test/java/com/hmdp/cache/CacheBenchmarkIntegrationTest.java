package com.hmdp.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.utils.CacheClient;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "citylens.reliability.scheduling-enabled=false",
      "hmdp.cdc.enabled=false",
      "logging.level.com.hmdp=info"
    })
class CacheBenchmarkIntegrationTest {
  @Autowired JdbcTemplate jdbc;
  @Autowired CacheClient cache;
  @Autowired TestRestTemplate http;
  @Autowired MeterRegistry metrics;
  private static final int REQUESTS = 10000;
  private static final int CONCURRENCY = 8;
  private static final int WARMUP = 200;

  private double loads() {
    Counter c =
        metrics.find("hmdp.cache.requests").tags("layer", "mysql", "result", "load").counter();
    return c == null ? 0 : c.count();
  }

  @Test
  void compareSameHttpEndpointWithCacheDisabledAndEnabled() throws Exception {
    assertEquals("hmdp_optimization_test", jdbc.queryForObject("SELECT DATABASE()", String.class));
    Map<String, Object> report = new LinkedHashMap<>();
    try {
      report.put("generated_at", Instant.now().toString());
      report.put("endpoint", "GET /shop/1");
      report.put("requests_per_phase", REQUESTS);
      report.put("concurrency", CONCURRENCY);
      report.put("warmup_per_phase", WARMUP);
      report.put(
          "environment",
          System.getProperty("os.name")
              + " "
              + System.getProperty("os.arch")
              + ", Java "
              + System.getProperty("java.version"));
      report.put("cpu_available_to_jvm", Runtime.getRuntime().availableProcessors());
      report.put("jvm_max_heap_bytes", Runtime.getRuntime().maxMemory());
      report.put("database", jdbc.queryForObject("SELECT VERSION()", String.class));
      report.put("baseline", phase(false));
      report.put("optimized", phase(true));
      report.put(
          "scope",
          "Isolated Docker MySQL 8 / Redis 7 / Memcached 1.6. One Java instance; "
              + "warm hot-key HTTP test, not a production capacity claim.");
      Files.createDirectories(Paths.get("docs"));
      new ObjectMapper()
          .writerWithDefaultPrettyPrinter()
          .writeValue(Paths.get("docs/cache-benchmark.json").toFile(), report);
    } finally {
      ReflectionTestUtils.setField(cache, "enabled", true);
      cache.invalidate("cache:shop:1");
    }
  }

  private Map<String, Object> phase(boolean enabled) throws Exception {
    ReflectionTestUtils.setField(cache, "enabled", enabled);
    cache.invalidate("cache:shop:1");
    double coldBefore = loads();
    request();
    double coldLoads = loads() - coldBefore;
    for (int i = 0; i < WARMUP; i++) {
      request();
    }
    double before = loads();
    ExecutorService pool = Executors.newFixedThreadPool(CONCURRENCY);
    CountDownLatch go = new CountDownLatch(1);
    List<Future<Long>> requests = new ArrayList<>();
    for (int i = 0; i < REQUESTS; i++) {
      requests.add(
          pool.submit(
              () -> {
                go.await();
                long started = System.nanoTime();
                request();
                return System.nanoTime() - started;
              }));
    }
    List<Long> latency = new ArrayList<>();
    long began = System.nanoTime();
    go.countDown();
    try {
      for (Future<Long> f : requests) {
        latency.add(f.get(30, TimeUnit.SECONDS));
      }
    } finally {
      pool.shutdownNow();
    }
    double seconds = (System.nanoTime() - began) / 1e9;
    double queries = loads() - before;
    Collections.sort(latency);
    if (enabled) {
      assertTrue(queries < REQUESTS / 10.0, "Cache did not reduce source reads");
    } else {
      assertEquals(REQUESTS, queries);
    }
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("successes", latency.size());
    out.put("errors", 0);
    out.put("duration_seconds", seconds);
    out.put("qps", REQUESTS / seconds);
    out.put("p50_ms", percentile(latency, 0.50));
    out.put("p95_ms", percentile(latency, 0.95));
    out.put("p99_ms", percentile(latency, 0.99));
    out.put("measured_mysql_detail_loads", queries);
    out.put("cold_request_mysql_detail_loads", coldLoads);
    return out;
  }

  @SuppressWarnings("unchecked")
  private void request() {
    ResponseEntity<Map> response = http.getForEntity("/shop/1", Map.class);
    assertEquals(200, response.getStatusCodeValue());
    assertEquals(true, response.getBody().get("success"));
    assertEquals(
        1, ((Number) ((Map<String, Object>) response.getBody().get("data")).get("id")).intValue());
  }

  private double percentile(List<Long> sorted, double p) {
    return sorted.get((int) Math.ceil(sorted.size() * p) - 1) / 1e6;
  }
}
