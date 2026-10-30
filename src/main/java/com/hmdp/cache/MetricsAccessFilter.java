package com.hmdp.cache;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/** Actuator uses its own handler mapping, so MVC interceptors do not secure these endpoints. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class MetricsAccessFilter extends OncePerRequestFilter {
  private final StringRedisTemplate redis;

  @Value("${hmdp.monitor-token:}")
  private String monitorToken;

  @Value("${citylens.catalog-admin-ids:}")
  private String admins;

  public MetricsAccessFilter(StringRedisTemplate redis) {
    this.redis = redis;
  }

  @Override
  protected boolean shouldNotFilter(HttpServletRequest request) {
    String path = request.getServletPath();
    if (path.isEmpty()) {
      path = request.getRequestURI().substring(request.getContextPath().length());
    }
    return !path.startsWith("/actuator/")
        || path.equals("/actuator/health")
        || path.equals("/actuator/health/");
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    String authorization = request.getHeader("Authorization");
    boolean authorized = false;
    if ("GET".equals(request.getMethod()) && authorization != null) {
      if (monitorToken != null && !monitorToken.isEmpty()) {
        authorized =
            MessageDigest.isEqual(
                ("Bearer " + monitorToken).getBytes(StandardCharsets.UTF_8),
                authorization.getBytes(StandardCharsets.UTF_8));
      }
      if (!authorized && admins != null && !admins.isEmpty()) {
        try {
          Object id = redis.opsForHash().get("login:token:" + authorization, "id");
          authorized =
              id != null
                  && Arrays.stream(admins.split(","))
                      .map(String::trim)
                      .anyMatch(admin -> admin.equals(id.toString()));
        } catch (RuntimeException unavailable) {
          response.sendError(503, "Monitoring authentication unavailable");
          return;
        }
      }
    }
    if (!authorized) {
      response.sendError(401, "Monitoring credential required");
      return;
    }
    chain.doFilter(request, response);
  }
}
