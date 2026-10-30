package com.hmdp.service;

import com.hmdp.entity.BusinessOutboxTask;
import java.util.List;

/** 持久业务任务的租约认领与投递；网络发送在认领事务提交之后执行。 */
public interface IOutboxRelayService {
  List<BusinessOutboxTask> claimOutboxBatch(String leaseOwner);

  void relayOutboxBatch();

  void redriveUnconsumedEvents();

  void refreshMetrics();
}
