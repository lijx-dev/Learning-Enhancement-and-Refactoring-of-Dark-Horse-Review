package com.hmdp.service;

import java.sql.Timestamp;

/** 业务事件写入与事务提交后的缓存通知。 */
public interface IBusinessOutboxService {
  void appendBusinessEvent(String kind, long aggregateId, Long userId);

  void appendMediaDeletionEvent(String path, long userId);

  void appendShopCacheInvalidation(long shopId);

  void appendOrderExpirationEvent(long orderId, Long userId, Timestamp deadline);

  void relayOutboxBatch();
}
