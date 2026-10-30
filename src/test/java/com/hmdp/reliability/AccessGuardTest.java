package com.hmdp.reliability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hmdp.dto.UserDTO;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.util.ReflectionTestUtils;

class AccessGuardTest {
  @AfterEach
  void clear() {
    UserHolder.removeUser();
  }

  @Test
  void publicReadsStayOpenButAnonymousAndNonAdminWritesAreRejected() throws Exception {
    AccessGuard guard = new AccessGuard(mock(RateLimiter.class));
    ReflectionTestUtils.setField(guard, "admins", "1010");
    assertTrue(
        guard.preHandle(
            new MockHttpServletRequest("GET", "/shop/1"),
            new MockHttpServletResponse(),
            new Object()));
    MockHttpServletResponse response = new MockHttpServletResponse();
    assertFalse(
        guard.preHandle(new MockHttpServletRequest("POST", "/shop"), response, new Object()));
    assertEquals(401, response.getStatus());
    UserDTO user = new UserDTO();
    user.setId(1012L);
    UserHolder.saveUser(user);
    response = new MockHttpServletResponse();
    assertFalse(
        guard.preHandle(new MockHttpServletRequest("PUT", "/shop"), response, new Object()));
    assertEquals(403, response.getStatus());
    user.setId(1010L);
    assertTrue(
        guard.preHandle(
            new MockHttpServletRequest("PUT", "/shop"),
            new MockHttpServletResponse(),
            new Object()));
    UserHolder.removeUser();
    response = new MockHttpServletResponse();
    assertFalse(
        guard.preHandle(
            new MockHttpServletRequest("DELETE", "/upload/blog"), response, new Object()));
  }

  @Test
  void experienceAllowsBoundedPublicSamplingButKeepsOrderDetailsAndWritesPrivate()
      throws Exception {
    RateLimiter limits = mock(RateLimiter.class);
    when(limits.allow(anyString(), eq(60), eq(60000L))).thenReturn(true);
    AccessGuard guard = new AccessGuard(limits);
    assertTrue(
        guard.preHandle(
            new MockHttpServletRequest("GET", "/experience/cache"),
            new MockHttpServletResponse(),
            new Object()));
    assertTrue(
        guard.preHandle(
            new MockHttpServletRequest("POST", "/experience/visits"),
            new MockHttpServletResponse(),
            new Object()));
    MockHttpServletResponse response = new MockHttpServletResponse();
    assertFalse(
        guard.preHandle(
            new MockHttpServletRequest("GET", "/experience/orders/10"), response, new Object()));
    assertEquals(401, response.getStatus());
    response = new MockHttpServletResponse();
    assertFalse(
        guard.preHandle(
            new MockHttpServletRequest("POST", "/experience/cache"), response, new Object()));
    assertEquals(401, response.getStatus());
  }

  @Test
  void publicExperienceIsRateLimitedBeforeDataAccess() throws Exception {
    AccessGuard guard = new AccessGuard(mock(RateLimiter.class));
    MockHttpServletResponse response = new MockHttpServletResponse();
    assertFalse(
        guard.preHandle(
            new MockHttpServletRequest("POST", "/experience/visits"), response, new Object()));
    assertEquals(429, response.getStatus());
  }

  @Test
  void monitoringCredentialOnlyGrantsReadAccessToMetrics() throws Exception {
    AccessGuard guard = new AccessGuard(mock(RateLimiter.class));
    ReflectionTestUtils.setField(guard, "monitorToken", "private-test-value");
    MockHttpServletRequest request = new MockHttpServletRequest("GET", "/actuator/prometheus");
    request.addHeader("Authorization", "Bearer private-test-value");
    assertTrue(guard.preHandle(request, new MockHttpServletResponse(), new Object()));
    request = new MockHttpServletRequest("GET", "/actuator/prometheus");
    request.addHeader("Authorization", "Bearer wrong");
    assertFalse(guard.preHandle(request, new MockHttpServletResponse(), new Object()));
    request = new MockHttpServletRequest("POST", "/shop");
    request.addHeader("Authorization", "Bearer private-test-value");
    assertFalse(guard.preHandle(request, new MockHttpServletResponse(), new Object()));
  }
}
