package com.hmdp.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class CacheReadTraceTest {
  @Test
  void nestedTraceRestoresOuterAndExceptionDoesNotLeakToNextRequest() {
    try (CacheReadTrace outer = CacheReadTrace.open()) {
      CacheReadTrace.record("jvm", "hit");
      assertThrows(
          IllegalStateException.class,
          () -> {
            try (CacheReadTrace inner = CacheReadTrace.open()) {
              CacheReadTrace.record("redis", "miss");
              assertEquals(1, inner.snapshot().size());
              throw new IllegalStateException("simulated read failure");
            }
          });
      CacheReadTrace.record("scope", "hit");
      assertEquals(2, outer.snapshot().size());
    }
    CacheReadTrace.record("mysql", "load");
    try (CacheReadTrace next = CacheReadTrace.open()) {
      assertTrue(next.snapshot().isEmpty());
    }
  }

  @Test
  void snapshotsCannotMutateTraceAndObservationsAreBounded() {
    try (CacheReadTrace trace = CacheReadTrace.open()) {
      CacheReadTrace.record("jvm", "hit");
      List<Map<String, String>> snapshot = trace.snapshot();
      snapshot.get(0).put("result", "miss");
      snapshot.clear();
      assertEquals("hit", trace.snapshot().get(0).get("result"));
      for (int i = 0; i < 100; i++) {
        CacheReadTrace.record("scope", "hit");
      }
      assertEquals(64, trace.snapshot().size());
    }
  }

  @Test
  void backgroundWorkerCannotContributeToRequestTrace() throws Exception {
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try (CacheReadTrace trace = CacheReadTrace.open()) {
      worker.submit(() -> CacheReadTrace.record("mysql", "load")).get(2, TimeUnit.SECONDS);
      assertTrue(trace.snapshot().isEmpty());
    } finally {
      worker.shutdownNow();
    }
  }
}
