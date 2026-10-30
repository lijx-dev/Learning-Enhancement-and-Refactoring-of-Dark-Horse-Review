package com.hmdp.enums;

/** 可靠任务状态；SENT 仅表示投递已确认，DONE 才表示消费完成。 */
public enum OutboxState {
  PENDING,
  SENDING,
  SENT,
  DONE,
  REVIEW;

  public boolean matches(String value) {
    return name().equals(value);
  }
}
