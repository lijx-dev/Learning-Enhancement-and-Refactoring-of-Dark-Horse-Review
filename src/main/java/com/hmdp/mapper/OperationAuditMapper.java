package com.hmdp.mapper;

import org.apache.ibatis.annotations.Param;

/** 数据访问接口，SQL 统一维护在对应的 Mapper XML 中。 */
public interface OperationAuditMapper {

  int insertAudit(
      @Param("userId") Long userId,
      @Param("method") String method,
      @Param("path") String path,
      @Param("httpStatus") Integer httpStatus);
}
