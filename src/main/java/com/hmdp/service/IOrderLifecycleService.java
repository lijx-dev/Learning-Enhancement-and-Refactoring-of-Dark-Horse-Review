package com.hmdp.service;

/** 订单创建、支付和超时关闭。 */
public interface IOrderLifecycleService {
  void registerOrderCreatedEventAndDeadline(long orderId);

  void registerOrderPaidEvent(long orderId, long userId);

  void registerOrderClosedEvent(long orderId, long userId);

  boolean closeExpiredOrdinaryOrder(long orderId);

  void expireOverdueOrders();
}
