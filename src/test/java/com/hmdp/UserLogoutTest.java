package com.hmdp;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.hmdp.controller.UserController;
import com.hmdp.dto.Result;
import com.hmdp.service.impl.UserServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

class UserLogoutTest {
  @Test
  void logoutRevokesOnlyPresentedToken() {
    StringRedisTemplate redis = mock(StringRedisTemplate.class);
    UserController controller = new UserController();
    UserServiceImpl service = new UserServiceImpl();
    ReflectionTestUtils.setField(service, "stringRedisTemplate", redis);
    ReflectionTestUtils.setField(controller, "userService", service);

    Result missing = controller.logout(null);
    assertFalse(missing.getSuccess());
    verifyNoInteractions(redis);

    Result result = controller.logout("test-token");
    assertTrue(result.getSuccess());
    verify(redis).delete("login:token:test-token");
    verifyNoMoreInteractions(redis);
  }
}
