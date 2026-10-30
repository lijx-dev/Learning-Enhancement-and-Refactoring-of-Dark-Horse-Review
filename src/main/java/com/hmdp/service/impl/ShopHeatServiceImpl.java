package com.hmdp.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.dto.Result;
import com.hmdp.service.IBehaviorAggregationService;
import com.hmdp.service.IShopHeatService;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class ShopHeatServiceImpl implements IShopHeatService {
  private final IBehaviorAggregationService aggregation;
  private final StringRedisTemplate redis;
  private final ObjectMapper json;

  @Value("${hmdp.analytics.enabled:false}")
  private boolean enabled;

  public ShopHeatServiceImpl(
      IBehaviorAggregationService aggregation, StringRedisTemplate redis, ObjectMapper json) {
    this.aggregation = aggregation;
    this.redis = redis;
    this.json = json;
  }

  @Scheduled(fixedDelay = 5000)
  @Override
  public void refresh() {
    if (!enabled) {
      return;
    }
    for (int window : new int[] {5, 60}) {
      try {
        redis
            .opsForValue()
            .set(
                "analytics:heat:" + window,
                json.writeValueAsString(aggregation.ranking(window, 100)),
                15,
                TimeUnit.SECONDS);
      } catch (Exception failure) {
        /* Durable buckets remain available for rebuilding the disposable snapshot. */
      }
    }
  }

  @Override
  public Result ranking(int windowMinutes, int limit) {
    if (!enabled) {
      return Result.fail("商家热度统计未启用");
    }
    if ((windowMinutes != 5 && windowMinutes != 60) || limit < 1 || limit > 100) {
      return Result.fail("窗口仅支持 5 或 60 分钟，条数 1～100");
    }
    List<Map<String, Object>> rows = null;
    try {
      String snapshot = redis.opsForValue().get("analytics:heat:" + windowMinutes);
      if (snapshot != null) {
        rows = json.readValue(snapshot, new TypeReference<List<Map<String, Object>>>() {});
      }
    } catch (Exception unavailable) {
      /* Rebuild from durable facts if Redis is unavailable. */
    }
    if (rows == null) {
      rows = aggregation.ranking(windowMinutes, limit);
    }
    return Result.ok(rows.subList(0, Math.min(limit, rows.size())));
  }
}
