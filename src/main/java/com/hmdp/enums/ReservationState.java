package com.hmdp.enums;

/** 持久预约与 Redis 请求投影使用的稳定状态值。 */
public enum ReservationState {
  PROCESSING,
  SUCCESS,
  REVIEW,
  FAILED;

  public boolean matches(String value) {
    return name().equals(value);
  }
}
