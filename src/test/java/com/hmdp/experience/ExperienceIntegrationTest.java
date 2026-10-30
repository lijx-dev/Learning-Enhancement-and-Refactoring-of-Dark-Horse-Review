package com.hmdp.experience;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.MessageMapper;
import com.hmdp.mapper.VoucherMapper;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "hmdp.experience.enabled=true", "hmdp.analytics.enabled=true",
      "hmdp.analytics.group=hmdp-experience-integration", "hmdp.mq.enabled=false",
      "citylens.reliability.scheduling-enabled=false"
    })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ExperienceIntegrationTest {
  @Autowired private TestRestTemplate http;
  @Autowired private ObjectMapper json;
  @Autowired private DataSource dataSource;
  @Autowired private StringRedisTemplate redis;
  @Autowired private VoucherMapper vouchers;
  @Autowired private MessageMapper messages;

  @BeforeEach
  void guardIsolation() throws Exception {
    try (Connection connection = dataSource.getConnection()) {
      assertEquals("hmdp_optimization_test", connection.getCatalog());
    }
    redis.delete("limit:experience-ip:127.0.0.1");
  }

  @Test
  void anonymousCacheReportsActualScopeHitsWhileOrdersAndAdministrationStayProtected()
      throws Exception {
    JsonNode caps = json.readTree(http.getForObject("/experience/capabilities", String.class));
    assertTrue(caps.path("data").path("enabled").asBoolean());
    assertFalse(caps.toString().contains("monitorToken"));
    JsonNode result = json.readTree(http.getForObject("/experience/cache?shopId=1", String.class));
    assertTrue(result.path("success").asBoolean());
    JsonNode reads = result.path("data").path("reads");
    assertEquals(2, reads.size());
    assertEquals("scope", reads.get(1).path("events").get(0).path("layer").asText());
    assertEquals("hit", reads.get(1).path("events").get(0).path("result").asText());
    assertEquals(401, http.getForEntity("/experience/orders/1", String.class).getStatusCodeValue());
    assertEquals(401, http.getForEntity("/ops/messages", String.class).getStatusCodeValue());
  }

  @Test
  void submittedVisitIdsBecomePersistedThroughRealKafka() throws Exception {
    JsonNode submitted =
        json.readTree(http.postForObject("/experience/visits?shopId=1", null, String.class));
    assertTrue(submitted.path("success").asBoolean());
    JsonNode ids = submitted.path("data").path("eventIds");
    assertEquals(3, ids.size());
    String query = ids.get(0).asText() + "," + ids.get(1).asText() + "," + ids.get(2).asText();
    long deadline = System.currentTimeMillis() + 30000;
    while (System.currentTimeMillis() < deadline) {
      JsonNode response =
          json.readTree(http.getForObject("/experience/visits?eventIds=" + query, String.class));
      assertTrue(response.path("success").asBoolean());
      JsonNode receipts = response.path("data");
      boolean complete = receipts.size() == 3;
      for (int i = 0; i < receipts.size(); i++) {
        JsonNode receipt = receipts.get(i);
        assertEquals(ids.get(i).asText(), receipt.path("eventId").asText());
        assertFalse(receipt.has("user_id"));
        complete &=
            "PERSISTED".equals(receipt.path("state").asText())
                && receipt.path("shopId").asLong() == 1;
      }
      if (complete) {
        return;
      }
      Thread.sleep(1000);
    }
    fail("本次三个浏览事件未在 30 秒内通过真实 Kafka 落库");
  }

  @Test
  void ownerSeesRealOutboxEventsAndAnotherAccountCannotReadThem() throws Exception {
    Voucher fixture =
        new Voucher()
            .setShopId(1L)
            .setTitle("experience-test")
            .setRules("test")
            .setPayValue(100L)
            .setActualValue(200L)
            .setType(0)
            .setStatus(1);
    vouchers.insert(fixture);
    long sku = fixture.getId();
    String owner = session(918102L);
    String other = session(918103L);
    try {
      String submitted =
          http.exchange(
                  "/voucher-order/ordinary/" + sku,
                  HttpMethod.POST,
                  authorized(owner),
                  String.class)
              .getBody();
      JsonNode created = json.readTree(submitted);
      assertTrue(created.path("success").asBoolean(), submitted);
      long orderId = created.path("data").asLong();
      String path = "/experience/orders/" + orderId;
      JsonNode progress =
          json.readTree(
              http.exchange(path, HttpMethod.GET, authorized(owner), String.class).getBody());
      assertTrue(progress.path("success").asBoolean());
      assertTrue(progress.path("data").size() > 0);
      assertFalse(progress.toString().contains("payload"));
      boolean createdEvent = false;
      for (JsonNode row : progress.path("data")) {
        createdEvent |= "ORDER_CREATED".equals(row.path("kind").asText());
      }
      assertTrue(createdEvent);
      JsonNode denied =
          json.readTree(
              http.exchange(path, HttpMethod.GET, authorized(other), String.class).getBody());
      assertFalse(denied.path("success").asBoolean());
      assertTrue(messages.selectOrderProgress(orderId, 918103L).isEmpty());
    } finally {
      redis.delete(LOGIN_USER_KEY + owner);
      redis.delete(LOGIN_USER_KEY + other);
    }
  }

  private String session(long userId) {
    String token = UUID.randomUUID().toString();
    Map<String, String> user = new HashMap<>();
    user.put("id", String.valueOf(userId));
    user.put("nickName", "isolated-test");
    redis.opsForHash().putAll(LOGIN_USER_KEY + token, user);
    redis.expire(LOGIN_USER_KEY + token, 5, TimeUnit.MINUTES);
    return token;
  }

  private HttpEntity<Void> authorized(String token) {
    HttpHeaders headers = new HttpHeaders();
    headers.set("Authorization", token);
    return new HttpEntity<>(headers);
  }
}
