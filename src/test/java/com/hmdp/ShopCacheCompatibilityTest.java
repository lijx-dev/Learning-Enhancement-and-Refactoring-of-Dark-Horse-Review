package com.hmdp;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

class ShopCacheCompatibilityTest {
  static class Service extends ShopServiceImpl {
    int reads;
    Shop database;

    @Override
    public Shop getById(Serializable id) {
      reads++;
      return database;
    }
  }

  @Test
  void repairsLogicalWrapperDecodedAsEmptyShop() {
    Service service = new Service();
    Shop actual = new Shop();
    actual.setId(1L);
    actual.setName("test");
    service.database = actual;
    CacheClient cache = mock(CacheClient.class);
    ReflectionTestUtils.setField(service, "cacheClient", cache);
    when(cache.queryWithPassThrough(
            eq(CACHE_SHOP_KEY),
            eq(1L),
            eq(Shop.class),
            any(),
            eq(CACHE_SHOP_TTL),
            eq(TimeUnit.MINUTES)))
        .thenReturn(new Shop());
    when(cache.queryWithMutex(
            eq(CACHE_SHOP_KEY),
            eq(1L),
            eq(Shop.class),
            any(),
            eq(CACHE_SHOP_TTL),
            eq(TimeUnit.MINUTES)))
        .thenAnswer(
            call -> {
              Function<Long, Shop> load = call.getArgument(3);
              return load.apply(1L);
            });
    Result result = service.queryById(1L);
    assertTrue(result.getSuccess());
    assertEquals(actual, result.getData());
    assertEquals(1, service.reads);
    verify(cache).invalidate(CACHE_SHOP_KEY + 1);
    verify(cache, never()).set(anyString(), any(), anyLong(), any());
  }

  @Test
  void validCachedShopDoesNotQueryDatabase() {
    Service service = new Service();
    Shop cached = new Shop();
    cached.setId(1L);
    CacheClient cache = mock(CacheClient.class);
    ReflectionTestUtils.setField(service, "cacheClient", cache);
    when(cache.queryWithPassThrough(
            eq(CACHE_SHOP_KEY),
            eq(1L),
            eq(Shop.class),
            any(),
            eq(CACHE_SHOP_TTL),
            eq(TimeUnit.MINUTES)))
        .thenReturn(cached);
    assertEquals(cached, service.queryById(1L).getData());
    assertEquals(0, service.reads);
  }
}
