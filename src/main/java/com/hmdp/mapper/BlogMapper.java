package com.hmdp.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hmdp.entity.Blog;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Param;

/**
 * Mapper 接口
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface BlogMapper extends BaseMapper<Blog> {

  Long countLike(@Param("blogId") Long blogId, @Param("userId") Long userId);

  List<Map<String, Object>> selectIdForUpdate(@Param("blogId") Long blogId);

  int insertLike(@Param("blogId") Long blogId, @Param("userId") Long userId);

  int incrementLikes(@Param("blogId") Long blogId);

  int deleteLike(@Param("blogId") Long blogId, @Param("userId") Long userId);

  int decrementLikes(@Param("blogId") Long blogId);

  List<Long> selectFirstLikeUserIds(@Param("blogId") Long blogId);

  List<Long> selectLikedBlogIds(@Param("userId") Long userId, @Param("blogIds") List<Long> blogIds);

  List<Map<String, Object>> selectLikeTimestamp(
      @Param("blogId") Long blogId, @Param("userId") Long userId);
}
