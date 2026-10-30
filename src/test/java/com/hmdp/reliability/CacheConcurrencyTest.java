package com.hmdp.reliability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hmdp.utils.CacheClient;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;

class CacheConcurrencyTest {
  @Test
  void failedLockAcquisitionDoesNotUnlockSomeoneElse() {
    StringRedisTemplate redis = mock(StringRedisTemplate.class);
    ValueOperations<String, String> values = mock(ValueOperations.class);
    when(redis.opsForValue()).thenReturn(values);
    when(values.setIfAbsent(anyString(), anyString(), eq(10L), eq(TimeUnit.SECONDS)))
        .thenReturn(false);
    CacheClient cache = new CacheClient(redis);
    AtomicInteger loads = new AtomicInteger();
    try {
      assertThrows(
          IllegalStateException.class,
          () ->
              cache.queryWithMutex(
                  "test:",
                  1L,
                  String.class,
                  id -> {
                    loads.incrementAndGet();
                    return "value";
                  },
                  1L,
                  TimeUnit.MINUTES));
      assertEquals(0, loads.get());
      verify(redis, never()).delete(anyString());
      verify(redis, never()).execute(any(RedisScript.class), anyList(), any(Object[].class));
    } finally {
      cache.close();
    }
  }
}
