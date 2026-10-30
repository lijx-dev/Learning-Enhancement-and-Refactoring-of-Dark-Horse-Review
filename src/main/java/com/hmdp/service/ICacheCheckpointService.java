package com.hmdp.service;

public interface ICacheCheckpointService {
  void saveCheckpoint(
      String destination, Long batchId, String binlogFile, Long binlogOffset, Long eventTimeMs);
}
