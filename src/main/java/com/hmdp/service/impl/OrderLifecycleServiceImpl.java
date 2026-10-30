package com.hmdp.service.impl;

import com.hmdp.enums.OrderStatus;
import com.hmdp.enums.VoucherType;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.IBusinessOutboxService;
import com.hmdp.service.IOrderLifecycleService;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import javax.annotation.Resource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class OrderLifecycleServiceImpl implements IOrderLifecycleService {

  @Resource
  private VoucherOrderMapper voucherOrderMapper;

  private final IBusinessOutboxService outbox;
  private final TransactionTemplate transactions;

  @Value("${citylens.reliability.order-timeout-minutes:30}")
  private int timeoutMinutes;

  public OrderLifecycleServiceImpl(
      IBusinessOutboxService outbox, TransactionTemplate transactions) {
    this.outbox = outbox;
    this.transactions = transactions;
  }

  /**
   * 在订单创建事务内初始化截止时间，并登记创建事件及延迟关单事件。
   *
   * <p>登记成功不代表已发送消息；Relay 会在业务提交后投递，回滚时事件也一起回滚。
   */
  @Override
  public void registerOrderCreatedEventAndDeadline(long orderId) {
    requireTransaction();
    if (timeoutMinutes < 1 || timeoutMinutes > 1440) {
      throw new IllegalStateException("普通订单超时必须在 1～1440 分钟之间");
    }
    voucherOrderMapper.initializeDeadline(timeoutMinutes, orderId);
    Map<String, Object> row = voucherOrderMapper.selectDeadline(orderId);
    Long userId = ((Number) row.get("user_id")).longValue();
    outbox.appendBusinessEvent("ORDER_CREATED", orderId, userId);
    if (row.get("expire_at") != null) {
      outbox.appendOrderExpirationEvent(orderId, userId, (Timestamp) row.get("expire_at"));
    }
  }

  @Override
  public void registerOrderPaidEvent(long orderId, long userId) {
    requireTransaction();
    outbox.appendBusinessEvent("ORDER_PAID", orderId, userId);
  }

  @Override
  public void registerOrderClosedEvent(long orderId, long userId) {
    requireTransaction();
    outbox.appendBusinessEvent("ORDER_CLOSED", orderId, userId);
  }

  /**
   * 锁定订单并重新校验当前状态、类型和截止时间，再条件关闭普通券订单。
   *
   * <p>提前到达的消息只重新登记截止事件；支付或关闭已经完成时不再产生重复关闭事件。
   */
  @Override
  public boolean closeExpiredOrdinaryOrder(long orderId) {
    return Boolean.TRUE.equals(
        transactions.execute(
            tx -> {
              List<Map<String, Object>> rows =
                  voucherOrderMapper.selectExpiryStateForUpdate(orderId);
              if (rows.isEmpty()) {
                return false;
              }
              Map<String, Object> row = rows.get(0);
              if (!OrderStatus.UNPAID.matches(((Number) row.get("status")).intValue())
                  || row.get("voucher_type_snapshot") == null
                  || !VoucherType.ORDINARY.matches(
                      ((Number) row.get("voucher_type_snapshot")).intValue())) {
                return false;
              }
              if (row.get("expire_at") == null) {
                throw new IllegalStateException("普通订单缺少截止时间");
              }
              long userId = ((Number) row.get("user_id")).longValue();
              if (((Number) row.get("due")).intValue() == 0) {
                outbox.appendOrderExpirationEvent(
                    orderId, userId, (Timestamp) row.get("expire_at"));
                return false;
              }
              if (voucherOrderMapper.closeExpiredOrder(orderId) == 0) {
                return false;
              }
              registerOrderClosedEvent(orderId, userId);
              return true;
            }));
  }

  private static void requireTransaction() {
    if (!TransactionSynchronizationManager.isActualTransactionActive()) {
      throw new IllegalStateException("订单状态与事件必须同事务提交");
    }
  }

  @Override
  public void expireOverdueOrders() {
    for (Long orderId : voucherOrderMapper.selectExpiredOrdinaryOrders()) {
      closeExpiredOrdinaryOrder(orderId);
    }
  }
}
