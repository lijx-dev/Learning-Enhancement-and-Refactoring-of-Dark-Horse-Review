package com.hmdp.service.impl;

import com.hmdp.cache.CacheReadTrace;
import com.hmdp.cache.RequestCacheScope;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.BehaviorMapper;
import com.hmdp.mapper.MessageMapper;
import com.hmdp.service.IBehaviorJournalService;
import com.hmdp.service.IExperienceService;
import com.hmdp.service.IShopService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.UserHolder;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Resource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ExperienceServiceImpl implements IExperienceService {
  @Resource
  private IShopService shopService;

  @Resource
  private IBehaviorJournalService behaviorJournalService;

  @Resource
  private IVoucherOrderService voucherOrderService;

  @Resource
  private BehaviorMapper behaviorMapper;

  @Resource
  private MessageMapper messageMapper;

  @Resource
  private MeterRegistry metrics;

  @Value("${hmdp.experience.enabled:false}")
  private boolean enabled;

  @Value("${hmdp.cache.enabled:true}")
  private boolean cacheEnabled;

  @Value("${hmdp.analytics.enabled:false}")
  private boolean analyticsEnabled;

  @Value("${hmdp.mq.enabled:false}")
  private boolean mqEnabled;

  @Value("${hmdp.cdc.enabled:false}")
  private boolean cdcEnabled;

  @Override
  public Result queryExperienceCapabilities() {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("enabled", enabled);
    if (enabled) {
      result.put("cacheEnabled", cacheEnabled);
      result.put("analyticsEnabled", analyticsEnabled);
      result.put("mqEnabled", mqEnabled);
      result.put("cdcEnabled", cdcEnabled);
      result.put("cdcConnected", readBooleanGauge("hmdp.cdc.connected"));
      result.put("mqClientStarted", readBooleanGauge("hmdp.mq.client.started"));
      result.put("kafkaConsumerRunning", readBooleanGauge("hmdp.kafka.consumer.running"));
      result.put("sampledAt", System.currentTimeMillis());
    }
    return Result.ok(result);
  }

  private Boolean readBooleanGauge(String name) {
    Gauge gauge = metrics.find(name).gauge();
    return gauge == null ? null : gauge.value() == 1;
  }

  /**
   * 在同一个请求缓存作用域中读取同一店铺两次，记录每次经过的缓存层与耗时。
   *
   * <p>用于观察请求级复用；不会主动清空缓存制造命中结果。
   */
  @Override
  public Result traceShopCacheReads(Long shopId) {
    if (!enabled) {
      return Result.fail("体验观察接口未启用");
    }
    if (shopId == null || shopId <= 0) {
      return Result.fail("请选择有效店铺");
    }
    List<Map<String, Object>> reads = new ArrayList<>();
    String shopName = null;
    try (RequestCacheScope scope = RequestCacheScope.open()) {
      for (int index = 0; index < 2; index++) {
        try (CacheReadTrace trace = CacheReadTrace.open()) {
          long started = System.nanoTime();
          Result loaded = shopService.queryById(shopId);
          if (!Boolean.TRUE.equals(loaded.getSuccess()) || loaded.getData() == null) {
            return loaded;
          }
          shopName = ((Shop) loaded.getData()).getName();
          Map<String, Object> read = new LinkedHashMap<>();
          read.put("sequence", index + 1);
          read.put("durationMicros", TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - started));
          read.put("events", trace.snapshot());
          reads.add(read);
        }
      }
    }
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("requestId", UUID.randomUUID().toString());
    result.put("shopId", shopId);
    result.put("shopName", shopName);
    result.put("reads", reads);
    return Result.ok(result);
  }

  /**
   * 提交三条体验访问事件并返回事件号，浏览器可继续轮询各事件的落库结果。
   *
   * <p>submitted 是提交采集的数量，不是 Kafka 确认数或成功写入数据库的数量。
   */
  @Override
  public Result submitDemoVisitEvents(Long shopId) {
    if (!enabled || !analyticsEnabled) {
      return Result.fail("Kafka 访问体验未启用");
    }
    if (shopId == null || shopId <= 0) {
      return Result.fail("请选择有效店铺");
    }
    Result shop = shopService.queryById(shopId);
    if (!Boolean.TRUE.equals(shop.getSuccess())) {
      return shop;
    }
    List<String> eventIds = new ArrayList<>();
    Long userId = UserHolder.getUser() == null ? null : UserHolder.getUser().getId();
    for (int index = 0; index < 3; index++) {
      String id = behaviorJournalService.submitShopVisitEvent(shopId, userId);
      if (id == null) {
        return Result.fail("访问采集未启用");
      }
      eventIds.add(id);
    }
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("eventIds", eventIds);
    result.put("submitted", eventIds.size());
    result.put("shopId", shopId);
    return Result.ok(result);
  }

  @Override
  public Result queryVisitEventReceipts(List<String> eventIds) {
    if (!enabled || !analyticsEnabled) {
      return Result.fail("Kafka 访问体验未启用");
    }
    if (eventIds == null || eventIds.isEmpty() || eventIds.size() > 3) {
      return Result.fail("一次仅查询 1～3 个体验事件");
    }
    for (String id : eventIds) {
      if (id == null || !id.matches("[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}")) {
        return Result.fail("体验事件号格式无效");
      }
    }
    List<Map<String, Object>> receipts = new ArrayList<>();
    for (String id : eventIds) {
      Map<String, Object> stored = behaviorMapper.selectEvent(id);
      Map<String, Object> receipt = new LinkedHashMap<>();
      receipt.put("eventId", id);
      receipt.put("state", stored == null ? "PENDING" : "PERSISTED");
      if (stored != null) {
        receipt.put("shopId", stored.get("shop_id"));
      }
      receipts.add(receipt);
    }
    return Result.ok(receipts);
  }

  @Override
  public Result queryMyOrderProgress(Long orderId) {
    if (!enabled) {
      return Result.fail("体验观察接口未启用");
    }
    if (orderId == null || orderId <= 0 || UserHolder.getUser() == null) {
      return Result.fail("请登录后查询自己的订单");
    }
    Result owned = voucherOrderService.queryMyOrder(orderId);
    if (!Boolean.TRUE.equals(owned.getSuccess())) {
      return owned;
    }
    return Result.ok(messageMapper.selectOrderProgress(orderId, UserHolder.getUser().getId()));
  }
}
