package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;

/** 秒杀请求持久化、投递与人工核对。 */
public interface ISeckillService {
  String STREAM = "stream.orders";
  String GROUP = "g1";

  void fulfillReservationFromMessage(long orderId);

  /** 在订单事务内锁预约；返回 false 表示已有终态，不能再次扣库存。 */
  boolean lockReservationForFulfillment(VoucherOrder order);

  void markReservationSuccessful(long orderId);

  String queryReservationState(long orderId);

  Result queryReservationStatus(long orderId, long userId);

  Result publishActivity(long voucherId);

  /** 人工核对后的恢复入口，与首次发布使用不同准入检查。 */
  Result recoverActivity(long voucherId);

  Result retryReservation(long orderId);

  Result markReservationFailedAndCompensateRedis(long orderId);

  /** 只生成对账报告，不自动修复库存。 */
  Result queryInventoryReconciliationReport(long voucherId);

  Result queryReservationsForReview();
}
