package com.hmdp.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hmdp.entity.Follow;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Param;

/**
 * Mapper 接口
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface FollowMapper extends BaseMapper<Follow> {

  int insertEdge(@Param("userId") Long userId, @Param("followUserId") Long followUserId);

  int deleteEdge(@Param("userId") Long userId, @Param("followUserId") Long followUserId);

  Long countEdge(@Param("userId") Long userId, @Param("followUserId") Long followUserId);

  List<Long> selectCommonFollowIds(
      @Param("userId") Long userId, @Param("otherUserId") Long otherUserId);

  List<Map<String, Object>> selectFanBatch(
      @Param("authorId") Long authorId,
      @Param("cursorId") Long cursorId,
      @Param("publishedAt") Timestamp publishedAt);
}
