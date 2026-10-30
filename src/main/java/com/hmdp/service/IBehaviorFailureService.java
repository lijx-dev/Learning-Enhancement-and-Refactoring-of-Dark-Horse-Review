package com.hmdp.service;

public interface IBehaviorFailureService {
  void saveFailure(
      String topic, Integer partitionId, Long messageOffset, String payload, String reason);
}
