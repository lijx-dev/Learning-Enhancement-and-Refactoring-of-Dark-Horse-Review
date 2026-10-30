package com.hmdp.mapper;

import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Param;

/** 数据访问接口，SQL 统一维护在对应的 Mapper XML 中。 */
public interface MediaMapper {

  int insertOwner(@Param("path") String path, @Param("userId") Long userId);

  List<Map<String, Object>> selectOwnerForUpdate(@Param("path") String path);

  int insertBlogReference(@Param("blogId") Long blogId, @Param("path") String path);

  List<Map<String, Object>> selectReferencesForUpdate(@Param("path") String path);

  int markDeleting(@Param("path") String path);

  List<String> selectOwnedStates(@Param("path") String path, @Param("userId") Long userId);

  int markDeleted(@Param("path") String path);
}
