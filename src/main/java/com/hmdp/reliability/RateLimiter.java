package com.hmdp.reliability;

import java.util.Collections;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

@Component
public class RateLimiter {
  private final StringRedisTemplate redis;
  private static final DefaultRedisScript<Long> SCRIPT = RedisScripts.number("rate-limit.lua");

  public RateLimiter(StringRedisTemplate redis) {
    this.redis = redis;
  }

  public boolean allow(String key, int limit, long windowMillis) {
    return Long.valueOf(1)
        .equals(
            redis.execute(
                SCRIPT,
                Collections.singletonList("limit:" + key),
                String.valueOf(limit),
                String.valueOf(windowMillis)));
  }
}
