package com.hmdp.entity;

import java.sql.Timestamp;
import lombok.Data;

/** 可靠业务任务的数据库快照；用于内部处理，不直接作为页面响应。 */
@Data
public class BusinessOutboxTask {
  private Long id;
  private String kind;
  private Long aggregateId;
  /** 发起用户；活动重试等无用户归属的任务可以为空。 */
  private Long userId;
  /** Feed 已处理到的关注关系行 ID，用于下一批查询；不是粉丝用户 ID。 */
  private long cursorId;

  private String state;
  private int attempts;
  private int consumeAttempts;
  /** 仅媒体删除任务使用，其他事件可以为空。 */
  private String resourcePath;
  /** 事件发生时间；Feed 续传沿用原值，防止排序时间随重试改变。 */
  private Timestamp createdAt;
  /** 延迟事件的目标执行时间；普通事件为空。 */
  private Timestamp deliverAt;
}
