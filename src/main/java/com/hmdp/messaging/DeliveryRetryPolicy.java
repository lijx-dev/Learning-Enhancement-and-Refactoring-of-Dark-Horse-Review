package com.hmdp.messaging;

/** 业务与分析 Outbox 共用的发送退避规则，保持现有重试次数和时间口径。 */
public final class DeliveryRetryPolicy {
  public static final int MAX_SEND_ATTEMPTS = 10;
  public static final int MAX_CONSUME_ATTEMPTS = 5;
  private static final long MAX_DELAY_SECONDS = 300;
  private static final int MAX_BACKOFF_EXPONENT = 8;

  private DeliveryRetryPolicy() {}

  /** 返回秒数；限制指数以避免溢出和无上限等待。 */
  public static long delaySeconds(int attempts) {
    return Math.min(MAX_DELAY_SECONDS, 1L << Math.min(attempts, MAX_BACKOFF_EXPONENT));
  }
}
