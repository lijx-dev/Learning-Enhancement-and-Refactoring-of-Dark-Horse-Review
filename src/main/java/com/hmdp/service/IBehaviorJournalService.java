package com.hmdp.service;

import com.hmdp.entity.BusinessOutboxTask;

/** 访问采集与持久订单分析事件的登记、投递。 */
public interface IBehaviorJournalService {
  void appendOrderBehaviorEvent(BusinessOutboxTask source);

  void recordShopVisit(long shopId, Long userId);

  /** 返回采集事件号，不代表已发送或已落库；未启用采集时返回 null。 */
  String submitShopVisitEvent(long shopId, Long userId);

  void relayAnalyticsOutboxBatch();

  void redriveMissingAnalyticsEvents();

  void refreshMetrics();
}
