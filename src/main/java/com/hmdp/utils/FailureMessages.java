package com.hmdp.utils;

/** 持久化失败原因的摘要，避免基础设施依赖某个业务 Service 实现类。 */
public final class FailureMessages {
  private static final int MAX_REASON_LENGTH = 500;

  private FailureMessages() {}

  public static String summarize(Throwable failure) {
    String text = failure.getClass().getSimpleName() + ": " + String.valueOf(failure.getMessage());
    return text.substring(0, Math.min(MAX_REASON_LENGTH, text.length()));
  }
}
