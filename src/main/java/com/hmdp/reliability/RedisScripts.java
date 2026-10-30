package com.hmdp.reliability;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;

public final class RedisScripts {
  private RedisScripts() {}

  public static DefaultRedisScript<Long> number(String path) {
    DefaultRedisScript<Long> script = new DefaultRedisScript<>();
    script.setLocation(new ClassPathResource(path));
    script.setResultType(Long.class);
    return script;
  }
}
