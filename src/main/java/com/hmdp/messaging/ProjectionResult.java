package com.hmdp.messaging;

/** 投影结果；CONTINUE 表示本批已成功完成，但还需要持久登记下一批任务。 */
public enum ProjectionResult {
  COMPLETE,
  CONTINUE
}
