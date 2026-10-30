package com.hmdp.enums;

/** 优惠券数据库类型码。 */
public enum VoucherType {
  ORDINARY(0),
  SECKILL(1);

  private final int code;

  VoucherType(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  public boolean matches(Integer value) {
    return value != null && value == code;
  }
}
