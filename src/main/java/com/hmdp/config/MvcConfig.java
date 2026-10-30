package com.hmdp.config;

import com.hmdp.reliability.AccessGuard;
import com.hmdp.utils.RefreshTokenInterceptor;
import javax.annotation.Resource;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class MvcConfig implements WebMvcConfigurer {

  @Resource
  private StringRedisTemplate stringRedisTemplate;

  @Resource
  private AccessGuard accessGuard;

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(accessGuard).addPathPatterns("/**").order(1);
    // token刷新的拦截器
    registry
        .addInterceptor(new RefreshTokenInterceptor(stringRedisTemplate))
        .addPathPatterns("/**")
        .order(0);
  }
}
