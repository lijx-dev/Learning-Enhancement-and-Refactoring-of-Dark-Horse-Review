package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;

/**
 * 服务类
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IBlogService extends IService<Blog> {

  Result queryHotBlog(Integer current);

  Result queryBlogById(Long id);

  Result likeBlog(Long id);

  Result setBlogLike(Long id, boolean liked);

  Result queryBlogLikes(Long id);

  Result saveBlog(Blog blog);

  Result queryBlogOfFollow(Long max, Integer offset);

  Result queryMyBlog(Integer current);

  Result queryBlogByShop(Long shopId, Integer current);

  Result searchBlogEvidence(Long shopId, String keyword, Integer current);

  Result queryBlogByUserId(Integer current, Long id);
}
