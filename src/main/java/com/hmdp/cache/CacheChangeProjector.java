package com.hmdp.cache;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hmdp.entity.Shop;
import com.hmdp.entity.Voucher;
import com.hmdp.service.IShopService;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.CacheClient;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CacheChangeProjector {
  private static final Set<String> WATCHED_TABLES =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList("tb_shop", "tb_voucher", "tb_seckill_voucher")));
  private static final long CDC_SHOP_TTL_MINUTES = 20;
  private static final long VOUCHER_METADATA_TTL_MINUTES = 10;

  private final CacheClient cache;
  private final ObjectProvider<IShopService> shopServiceProvider;
  private final ObjectProvider<IVoucherService> voucherServiceProvider;
  private final MeterRegistry metrics;

  @Value("${hmdp.cdc.schema:hmdp}")
  private String schema;

  public CacheChangeProjector(
      CacheClient cache,
      ObjectProvider<IShopService> shopServiceProvider,
      ObjectProvider<IVoucherService> voucherServiceProvider,
      MeterRegistry metrics) {
    this.cache = cache;
    this.shopServiceProvider = shopServiceProvider;
    this.voucherServiceProvider = voucherServiceProvider;
    this.metrics = metrics;
  }

  public boolean isRelevantEntry(CanalEntry.Entry entry) {
    return entry.getEntryType() == CanalEntry.EntryType.ROWDATA
        && schema.equals(entry.getHeader().getSchemaName())
        && WATCHED_TABLES.contains(entry.getHeader().getTableName());
  }

  /**
   * 仅处理指定数据库和表的变更，失效相关元数据缓存并按需回源。
   *
   * <p>活动执行库存和购买资格由秒杀流程维护，CDC 不用数据库快照覆盖 Redis 执行状态。
   */
  public void projectEntryChanges(CanalEntry.Entry entry) throws InvalidProtocolBufferException {
    log.debug(
        "CDC entry: {} {}.{} type={}",
        entry.getEntryType(),
        entry.getHeader().getSchemaName(),
        entry.getHeader().getTableName(),
        entry.getHeader().getEventType());
    if (!isRelevantEntry(entry)) {
      return;
    }
    String table = entry.getHeader().getTableName();
    CanalEntry.RowChange change = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
    if (change.getIsDdl()) {
      invalidateTableMetadata(table);
      metrics.counter("hmdp.cdc.ddl", "table", table).increment();
      return;
    }
    log.debug("CDC row count={} event={}", change.getRowDatasCount(), change.getEventType());
    for (CanalEntry.RowData row : change.getRowDatasList()) {
      projectRow(table, change.getEventType(), row);
      metrics.counter("hmdp.cdc.rows", "table", table).increment();
    }
  }

  private void invalidateTableMetadata(String table) {
    if ("tb_shop".equals(table)) {
      cache.invalidatePrefix("cache:shop:");
    }
    if ("tb_voucher".equals(table)) {
      cache.invalidatePrefix("cache:voucher-meta:");
      cache.invalidatePrefix("cache:voucher-list:");
    }
  }

  private void projectRow(String table, CanalEntry.EventType eventType, CanalEntry.RowData row) {
    Map<String, String> before = mapColumnValues(row.getBeforeColumnsList());
    Map<String, String> after = mapColumnValues(row.getAfterColumnsList());
    Map<String, String> identity = after.isEmpty() ? before : after;
    if ("tb_shop".equals(table)) {
      projectShopChange(parseRowId(identity, "id"), eventType);
    } else if ("tb_voucher".equals(table)) {
      projectVoucherChange(parseRowId(identity, "id"), eventType, before, after);
    }
    // 活动库存与窗口实时查库；CDC 不覆盖 Redis 的执行库存或购买资格。
  }

  private void projectShopChange(long shopId, CanalEntry.EventType eventType) {
    cache.invalidate("cache:shop:" + shopId);
    if (eventType != CanalEntry.EventType.DELETE) {
      cache.queryWithMutex(
          "cache:shop:",
          shopId,
          Shop.class,
          id -> shopServiceProvider.getObject().getById(id),
          CDC_SHOP_TTL_MINUTES,
          TimeUnit.MINUTES);
    }
  }

  private void projectVoucherChange(
      long voucherId,
      CanalEntry.EventType eventType,
      Map<String, String> before,
      Map<String, String> after) {
    cache.invalidate("cache:voucher-meta:" + voucherId);
    // 券换店后旧、新店铺都需要失效；这里只对当前行的关联店铺去重。
    Set<String> shopIds = new HashSet<>();
    shopIds.add(before.get("shop_id"));
    shopIds.add(after.get("shop_id"));
    shopIds.remove(null);
    for (String shopId : shopIds) {
      cache.invalidate("cache:voucher-list:" + Long.parseLong(shopId));
    }
    if (eventType != CanalEntry.EventType.DELETE) {
      cache.queryWithMutex(
          "cache:voucher-meta:",
          voucherId,
          Voucher.class,
          id -> voucherServiceProvider.getObject().getById(id),
          VOUCHER_METADATA_TTL_MINUTES,
          TimeUnit.MINUTES);
    }
  }

  private static Map<String, String> mapColumnValues(List<CanalEntry.Column> columns) {
    Map<String, String> result = new HashMap<>();
    for (CanalEntry.Column column : columns) {
      if (!column.getIsNull()) {
        result.put(column.getName(), column.getValue());
      }
    }
    return result;
  }

  private static long parseRowId(Map<String, String> row, String key) {
    String value = row.get(key);
    if (value == null) {
      throw new IllegalStateException("binlog 缺少完整主键: " + key);
    }
    return Long.parseLong(value);
  }
}
