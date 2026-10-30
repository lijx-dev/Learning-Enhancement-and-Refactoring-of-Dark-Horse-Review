package com.hmdp.mapper;

import org.apache.ibatis.annotations.Param;

/** 数据访问接口，SQL 统一维护在对应的 Mapper XML 中。 */
public interface CacheCheckpointMapper {

  int saveCheckpoint(
      @Param("destination") String destination,
      @Param("batchId") Long batchId,
      @Param("binlogFile") String binlogFile,
      @Param("binlogOffset") Long binlogOffset,
      @Param("eventTimeMs") Long eventTimeMs);
}
