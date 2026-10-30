package com.hmdp.enums;

/** 订单数据库状态码；免费券也使用 PAID 表示已领取，不能据此推断真实收款。 */
public enum OrderStatus {
  UNPAID(1),
  PAID(2),
  USED(3),
  CANCELLED(4),
  REFUNDING(5),
  REFUNDED(6);

  private final int code;

  OrderStatus(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  public boolean matches(Integer value) {
    return value != null && value == code;
  }
}
