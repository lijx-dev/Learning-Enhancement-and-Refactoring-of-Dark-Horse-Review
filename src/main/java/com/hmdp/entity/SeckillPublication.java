package com.hmdp.entity;

import java.sql.Timestamp;
import lombok.Data;

/** 活动发布所需的当前库存、时间窗口和上架状态。 */
@Data
public class SeckillPublication {
  private Long voucherId;
  private Integer stock;
  private Timestamp beginTime;
  private Timestamp endTime;
  private Integer status;
}
