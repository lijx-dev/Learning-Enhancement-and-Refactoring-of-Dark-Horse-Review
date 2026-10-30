package com.hmdp.service;

import com.hmdp.dto.Result;
import java.util.List;

/** Bounded experience observations; no cache eviction or administrative operations. */
public interface IExperienceService {
  Result queryExperienceCapabilities();

  Result traceShopCacheReads(Long shopId);

  Result submitDemoVisitEvents(Long shopId);

  Result queryVisitEventReceipts(List<String> eventIds);

  Result queryMyOrderProgress(Long orderId);
}
