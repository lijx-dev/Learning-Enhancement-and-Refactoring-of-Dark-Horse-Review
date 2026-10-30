package com.hmdp.entity;

import lombok.Data;

/** 秒杀持久资格；订单、用户与券的身份必须在重放时保持一致。 */
@Data
public class SeckillReservation {
  private Long orderId;
  private Long voucherId;
  private Long userId;
  private String state;
  private int attempts;
  private String reason;
}
