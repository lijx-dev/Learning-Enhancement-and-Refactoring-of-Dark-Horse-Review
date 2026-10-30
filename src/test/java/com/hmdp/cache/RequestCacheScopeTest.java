package com.hmdp.cache;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.servlet.ServletException;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

class RequestCacheScopeTest {
  @Test
  void filterCleansUpEvenOnExceptionAndNextRequestStartsEmpty() throws Exception {
    RequestCacheFilter filter = new RequestCacheFilter();
    assertThrows(
        ServletException.class,
        () ->
            filter.doFilter(
                new MockHttpServletRequest(),
                new MockHttpServletResponse(),
                (req, res) -> {
                  assertTrue(RequestCacheScope.active());
                  RequestCacheScope.put("one", new CacheSnapshot("{}", Long.MAX_VALUE, true));
                  throw new ServletException("test failure");
                }));
    assertFalse(RequestCacheScope.active());
    assertNull(RequestCacheScope.get("one"));
    filter.doFilter(
        new MockHttpServletRequest(),
        new MockHttpServletResponse(),
        (req, res) -> assertNull(RequestCacheScope.get("one")));
    assertFalse(RequestCacheScope.active());
  }

  @Test
  void nestedScopeRestoresOuterAndNeverLeaksIntoWorker() throws Exception {
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try (RequestCacheScope outer = RequestCacheScope.open()) {
      CacheSnapshot original = new CacheSnapshot("{}", Long.MAX_VALUE, true);
      RequestCacheScope.put("one", original);
      try (RequestCacheScope inner = RequestCacheScope.open()) {
        assertNull(RequestCacheScope.get("one"));
      }
      assertSame(original, RequestCacheScope.get("one"));
      assertFalse(worker.submit(RequestCacheScope::active).get(2, TimeUnit.SECONDS));
    } finally {
      worker.shutdownNow();
    }
    assertFalse(RequestCacheScope.active());
  }
}
