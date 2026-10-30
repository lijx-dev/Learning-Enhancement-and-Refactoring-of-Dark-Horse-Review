package com.hmdp.analytics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.hmdp.dto.UserDTO;
import com.hmdp.service.IShopHeatService;
import com.hmdp.service.impl.BehaviorJournalServiceImpl;
import com.hmdp.service.impl.OutboxRelayServiceImpl;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import com.hmdp.utils.UserHolder;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "citylens.reliability.scheduling-enabled=false",
      "hmdp.mq.enabled=false",
      "hmdp.analytics.enabled=true",
      "hmdp.analytics.group=hmdp-integration-heat"
    })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class KafkaBehaviorIntegrationTest {
  @Autowired JdbcTemplate jdbc;
  @Autowired KafkaBehaviorTransport transport;
  @Autowired BehaviorJournalServiceImpl journal;
  @Autowired OutboxRelayServiceImpl relay;
  @Autowired VoucherOrderServiceImpl orders;
  @Autowired ShopHeatController ranking;
  @Autowired IShopHeatService heat;
  @Autowired TestRestTemplate http;
  private static final long SKU = 917001;
  private static final long USER = 917002;

  @BeforeEach
  void guard() {
    assertEquals("hmdp_optimization_test", jdbc.queryForObject("SELECT DATABASE()", String.class));
  }

  private static void await(BooleanSupplier check) throws InterruptedException {
    long until = System.currentTimeMillis() + 30000;
    while (System.currentTimeMillis() < until) {
      if (check.getAsBoolean()) {
        return;
      }
      Thread.sleep(100);
    }
    fail("真实 Kafka 消费未在 30 秒内完成");
  }

  @Test
  void realKafkaRedeliveryIsIdempotentAndHttpVisitIsCollected() throws Exception {
    BehaviorEvent event =
        new BehaviorEvent(
            "real-kafka-" + System.nanoTime(), 1, USER, "VISIT", System.currentTimeMillis());
    for (int i = 0; i < 5; i++) {
      transport.publishAndAwaitAcknowledgement(event);
    }
    await(
        () ->
            jdbc.queryForObject(
                    "SELECT COUNT(*) FROM tb_behavior_event WHERE event_id=?",
                    Long.class,
                    event.getEventId())
                == 1);
    long before =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM tb_behavior_event WHERE action='VISIT' AND user_id IS NULL",
            Long.class);
    assertEquals(200, http.getForEntity("/shop/1", String.class).getStatusCodeValue());
    await(
        () ->
            jdbc.queryForObject(
                    "SELECT COUNT(*) FROM tb_behavior_event WHERE action='VISIT' AND "
                        + "user_id IS NULL",
                    Long.class)
                > before);
    heat.refresh();
    assertTrue(
        http.getForObject("/shop/hot-ranking?windowMinutes=5", String.class).contains("logged_uv"));
  }

  @Test
  void orderAndMockPaymentReachKafkaThroughDurableAnalyticsOutbox() throws Exception {
    jdbc.update("DELETE FROM tb_voucher_order WHERE voucher_id=?", SKU);
    jdbc.update("DELETE FROM tb_voucher WHERE id=?", SKU);
    jdbc.update(
        "INSERT INTO "
            + "tb_voucher(id,shop_id,title,rules,pay_value,actual_value,type,status) "
            + "VALUES(?,1,'kafka-test','rules',100,200,0,1)",
        SKU);
    UserDTO user = new UserDTO();
    user.setId(USER);
    UserHolder.saveUser(user);
    long id;
    try {
      id = ((Number) orders.orderOrdinaryVoucher(SKU).getData()).longValue();
      assertTrue(orders.payMockOrder(id, 100L).getSuccess());
    } finally {
      UserHolder.removeUser();
    }
    relay.relayOutboxBatch();
    journal.relayAnalyticsOutboxBatch();
    await(
        () ->
            jdbc.queryForObject(
                    "SELECT COUNT(*) FROM tb_behavior_event WHERE event_id IN (SELECT "
                        + "CONCAT('order:',id) FROM tb_business_outbox WHERE aggregate_id=? AND "
                        + "kind IN ('ORDER_CREATED','ORDER_PAID'))",
                    Long.class,
                    id)
                == 2);
    assertEquals(
        1L,
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM tb_behavior_outbox WHERE event_id IN (SELECT "
                + "CONCAT('order:',id) FROM tb_business_outbox WHERE aggregate_id=? AND "
                + "kind='ORDER_CREATED') AND state='SENT'",
            Long.class,
            id));
    journal.relayAnalyticsOutboxBatch();
    assertEquals(
        2L,
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM tb_behavior_event WHERE event_id IN (SELECT "
                + "CONCAT('order:',id) FROM tb_business_outbox WHERE aggregate_id=? AND "
                + "kind IN ('ORDER_CREATED','ORDER_PAID'))",
            Long.class,
            id));
  }
}
