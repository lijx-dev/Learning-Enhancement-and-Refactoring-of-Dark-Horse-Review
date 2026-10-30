package com.hmdp.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.BusinessOutboxTask;
import com.hmdp.mapper.BusinessOutboxMapper;
import com.hmdp.reliability.OrderMaintenance;
import com.hmdp.service.impl.BusinessEventServiceImpl;
import com.hmdp.service.impl.BusinessOutboxServiceImpl;
import com.hmdp.service.impl.OrderLifecycleServiceImpl;
import com.hmdp.service.impl.OutboxRelayServiceImpl;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import com.hmdp.utils.UserHolder;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest(
    properties = {"citylens.reliability.scheduling-enabled=false", "hmdp.mq.enabled=false"})
class MessagingIntegrationTest {
  @Autowired JdbcTemplate jdbc;
  @Autowired StringRedisTemplate redis;
  @Autowired BusinessOutboxMapper businessOutboxMapper;
  @Autowired TransactionTemplate transactions;
  @Autowired BusinessOutboxServiceImpl outbox;
  @Autowired OutboxRelayServiceImpl relay;
  @Autowired BusinessEventServiceImpl consumer;
  @Autowired OrderLifecycleServiceImpl lifecycle;
  @Autowired OrderMaintenance maintenance;
  @Autowired VoucherOrderServiceImpl orders;
  @MockBean EventPublisher publisher;
  private static final long SKU = 915001;
  private static final long USER = 915002;

  @BeforeEach
  void isolate() {
    assertEquals("hmdp_optimization_test", jdbc.queryForObject("SELECT DATABASE()", String.class));
    jdbc.update("DELETE FROM tb_message_inbox");
    jdbc.update("DELETE FROM tb_message_failure");
    jdbc.update("DELETE FROM tb_order_event_history");
    jdbc.update("DELETE FROM tb_business_outbox");
    jdbc.update("DELETE FROM tb_voucher_order WHERE voucher_id=?", SKU);
    jdbc.update("DELETE FROM tb_voucher WHERE id=?", SKU);
    jdbc.update(
        "INSERT INTO "
            + "tb_voucher(id,shop_id,title,rules,pay_value,actual_value,type,status) "
            + "VALUES(?,1,'mq-test','rules',100,200,0,1)",
        SKU);
    ReflectionTestUtils.setField(relay, "mqEnabled", false);
    UserDTO user = new UserDTO();
    user.setId(USER);
    UserHolder.saveUser(user);
  }

  @AfterEach
  void clear() {
    UserHolder.removeUser();
    ReflectionTestUtils.setField(relay, "mqEnabled", false);
  }

  private long append(String kind, long id) {
    transactions.execute(
        tx -> {
          outbox.appendBusinessEvent(kind, id, USER);
          return null;
        });
    return jdbc.queryForObject("SELECT MAX(id) FROM tb_business_outbox", Long.class);
  }

  private BusinessEvent event(long id) {
    return BusinessEvent.from(businessOutboxMapper.selectEventById(id));
  }

  private String state(long id) {
    return jdbc.queryForObject("SELECT state FROM tb_business_outbox WHERE id=?", String.class, id);
  }

  private long order() {
    return ((Number) orders.orderOrdinaryVoucher(SKU).getData()).longValue();
  }

  @Test
  void businessRollbackAlsoRollsBackEvent() {
    assertThrows(
        IllegalStateException.class,
        () ->
            transactions.execute(
                tx -> {
                  outbox.appendBusinessEvent("ORDER_CREATED", 1, USER);
                  throw new IllegalStateException("rollback");
                }));
    assertEquals(0L, jdbc.queryForObject("SELECT COUNT(*) FROM tb_business_outbox", Long.class));
    assertThrows(
        IllegalStateException.class, () -> outbox.appendBusinessEvent("ORDER_CREATED", 1, USER));
  }

  @Test
  void parallelRelaysClaimDisjointEventsAndExpiredLeaseCanBeRecovered() throws Exception {
    for (int i = 0; i < 45; i++) {
      append("ORDER_CREATED", i + 1);
    }
    ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      List<Future<List<BusinessOutboxTask>>> results = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        final String owner = "relay-" + i;
        results.add(pool.submit(() -> relay.claimOutboxBatch(owner)));
      }
      Set<Long> ids = new HashSet<>();
      for (Future<List<BusinessOutboxTask>> result : results) {
        for (BusinessOutboxTask task : result.get(10, TimeUnit.SECONDS)) {
          assertTrue(ids.add(task.getId()));
        }
      }
      assertEquals(45, ids.size());
      assertTrue(relay.claimOutboxBatch("new-owner").isEmpty());
      long expired = ids.iterator().next();
      jdbc.update(
          "UPDATE tb_business_outbox SET lease_until=TIMESTAMPADD(SECOND,-1,NOW()) WHERE id=?",
          expired);
      assertEquals(expired, relay.claimOutboxBatch("recovery").get(0).getId().longValue());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void feedContinuationPreservesCursorAndPublicationTimeAcrossDuplicateDelivery() {
    long blogId = 915003;
    long firstFollowerId = 916000;
    List<String> feedKeys = new ArrayList<>();
    jdbc.update("DELETE FROM tb_follow_edge WHERE follow_user_id=?", USER);
    try {
      for (int i = 0; i < 202; i++) {
        long followerId = firstFollowerId + i;
        feedKeys.add("feed:" + followerId);
        // 201 人在发布前关注；最后一人发布后才关注，不应收到这条历史笔记。
        jdbc.update(
            "INSERT INTO tb_follow_edge(user_id,follow_user_id,created_at) "
                + "VALUES(?,?,TIMESTAMPADD(SECOND,?,NOW()))",
            followerId,
            USER,
            i < 201 ? -5 : 86400);
      }
      long eventId = append("FEED", blogId);
      BusinessEvent firstBatch = event(eventId);
      consumer.consume(firstBatch, "feed");
      long continuationId =
          jdbc.queryForObject("SELECT MAX(id) FROM tb_business_outbox", Long.class);
      assertTrue(continuationId > eventId);
      BusinessOutboxTask continuation = businessOutboxMapper.selectEventById(continuationId);
      assertTrue(continuation.getCursorId() > 0);
      assertEquals(USER, continuation.getUserId().longValue());
      assertEquals(firstBatch.getOccurredAt(), continuation.getCreatedAt().getTime());
      assertNotNull(redis.opsForZSet().score(feedKeys.get(199), String.valueOf(blogId)));
      assertNull(redis.opsForZSet().score(feedKeys.get(200), String.valueOf(blogId)));

      consumer.consume(firstBatch, "feed");
      assertEquals(2L, jdbc.queryForObject("SELECT COUNT(*) FROM tb_business_outbox", Long.class));
      consumer.consume(event(continuationId), "feed");
      assertEquals(
          Double.valueOf(firstBatch.getOccurredAt()),
          redis.opsForZSet().score(feedKeys.get(200), String.valueOf(blogId)));
      assertNull(redis.opsForZSet().score(feedKeys.get(201), String.valueOf(blogId)));
      assertEquals("DONE", state(eventId));
      assertEquals("DONE", state(continuationId));
      assertEquals(2L, jdbc.queryForObject("SELECT COUNT(*) FROM tb_message_inbox", Long.class));
    } finally {
      jdbc.update("DELETE FROM tb_follow_edge WHERE follow_user_id=?", USER);
      redis.delete(feedKeys);
    }
  }

  @Test
  void unknownSendResultRetriesWithSameEventId() {
    long id = append("ORDER_CREATED", 100);
    ReflectionTestUtils.setField(relay, "mqEnabled", true);
    doThrow(new IllegalStateException("broker accepted but ACK lost"))
        .doNothing()
        .when(publisher)
        .publish(any());
    relay.relayOutboxBatch();
    assertEquals("PENDING", state(id));
    jdbc.update("UPDATE tb_business_outbox SET next_attempt_at=NOW() WHERE id=?", id);
    relay.relayOutboxBatch();
    assertEquals("SENT", state(id));
    ArgumentCaptor<BusinessEvent> sent = ArgumentCaptor.forClass(BusinessEvent.class);
    verify(publisher, times(2)).publish(sent.capture());
    assertEquals(id, sent.getAllValues().get(0).getEventId());
    assertEquals(id, sent.getAllValues().get(1).getEventId());
    consumer.consume(event(id), "orders");
    assertEquals("DONE", state(id));
  }

  @Test
  void consumerCanCompleteBeforePublisherMarksSent() {
    long id = append("ORDER_CREATED", 101);
    ReflectionTestUtils.setField(relay, "mqEnabled", true);
    doAnswer(
            call -> {
              consumer.consume(call.getArgument(0), "orders");
              return null;
            })
        .when(publisher)
        .publish(any());
    relay.relayOutboxBatch();
    assertEquals("DONE", state(id));
  }

  @Test
  void lostBrokerMessageCanBeRedrivenButFutureDeadlineIsPreserved() {
    long missing = append("ORDER_CREATED", 105);
    long future = append("ORDER_EXPIRE", 106);

    jdbc.update(
        "UPDATE tb_business_outbox SET "
            + "state='SENT',sent_at=TIMESTAMPADD(MINUTE,-10,NOW()) WHERE id IN (?,?)",
        missing,
        future);
    jdbc.update(
        "UPDATE tb_business_outbox SET deliver_at=TIMESTAMPADD(MINUTE,30,NOW()) WHERE id=?",
        future);
    relay.redriveUnconsumedEvents();
    assertEquals("PENDING", state(missing));
    assertEquals("SENT", state(future));
    relay.relayOutboxBatch();
    assertEquals("DONE", state(missing));
  }

  @Test
  void failedFulfillmentRetainsReservationAndCreatesReviewTask() {
    long order = 915099;
    jdbc.update("DELETE FROM tb_seckill_reservation WHERE order_id=?", order);
    jdbc.update(
        "INSERT INTO tb_seckill_reservation(order_id,voucher_id,user_id) VALUES(?,?,?)",
        order,
        SKU,
        USER);
    long id = append("SECKILL_FULFILL", order);
    assertTrue(
        consumer.recordConsumptionFailure(
            event(id),
            "fulfillment",
            "failed-fulfillment",
            "{}",
            new IllegalStateException("database stock conflict"),
            5));
    assertEquals("REVIEW", state(id));
    assertEquals(
        "REVIEW",
        jdbc.queryForObject(
            "SELECT state FROM tb_seckill_reservation WHERE order_id=?", String.class, order));
    assertEquals(
        0L,
        jdbc.queryForObject("SELECT COUNT(*) FROM tb_voucher_order WHERE id=?", Long.class, order));
    jdbc.update("DELETE FROM tb_seckill_reservation WHERE order_id=?", order);
  }

  @Test
  void concurrentDuplicateDeliveryCreatesOneHistoryAndOneInbox() throws Exception {
    long id = append("ORDER_PAID", 102);
    BusinessEvent message = event(id);
    ExecutorService pool = Executors.newFixedThreadPool(8);
    try {
      List<Future<?>> results = new ArrayList<>();
      for (int i = 0; i < 16; i++) {
        results.add(pool.submit(() -> consumer.consume(message, "orders")));
      }
      for (Future<?> result : results) {
        result.get(15, TimeUnit.SECONDS);
      }
      assertEquals(
          1L,
          jdbc.queryForObject(
              "SELECT COUNT(*) FROM tb_order_event_history WHERE event_id=?", Long.class, id));
      assertEquals(
          1L,
          jdbc.queryForObject(
              "SELECT COUNT(*) FROM tb_message_inbox WHERE event_id=?", Long.class, id));
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void invalidPayloadCannotQuarantineAnotherBusinessEvent() {
    long id = append("ORDER_CREATED", 103);
    BusinessEvent wrong = event(id);
    wrong.setAggregateId(999);
    assertThrows(IllegalArgumentException.class, () -> consumer.consume(wrong, "orders"));
    assertTrue(
        consumer.recordConsumptionFailure(
            wrong, "orders", "invalid-1", "{}", new IllegalArgumentException("conflict"), 5));
    assertEquals("PENDING", state(id));
    assertNull(
        jdbc.queryForObject(
            "SELECT event_id FROM tb_message_failure WHERE event_key='broker:invalid-1'",
            Long.class));
  }

  @Test
  void persistentFailureIsQuarantinedAndManualRetryPreservesIdentity() {
    long id = append("ORDER_CREATED", 104);
    BusinessEvent message = event(id);
    for (int attempt = 1; attempt <= 5; attempt++) {
      assertEquals(
          attempt == 5,
          consumer.recordConsumptionFailure(
              message,
              "orders",
              "failure-1",
              "{}",
              new IllegalStateException("storage unavailable"),
              attempt));
    }
    assertEquals("REVIEW", state(id));
    assertThrows(IllegalStateException.class, () -> consumer.consume(message, "orders"));
    jdbc.update(
        "UPDATE tb_business_outbox SET "
            + "state='PENDING',attempts=0,consume_attempts=0,next_attempt_at=NOW() "
            + "WHERE id=?",
        id);
    relay.relayOutboxBatch();
    assertEquals("DONE", state(id));
  }

  @Test
  void deadlineIsPersistedAndPaidOrderSurvivesLateDelivery() {
    long id = order();
    assertNotNull(
        jdbc.queryForObject(
            "SELECT expire_at FROM tb_voucher_order WHERE id=?", Timestamp.class, id));
    assertEquals(
        1L,
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM tb_business_outbox WHERE aggregate_id=? AND kind='ORDER_EXPIRE'",
            Long.class,
            id));
    assertTrue(orders.payMockOrder(id, 100L).getSuccess());
    assertTrue(orders.payMockOrder(id, 100L).getSuccess());
    jdbc.update(
        "UPDATE tb_voucher_order SET expire_at=TIMESTAMPADD(SECOND,-1,NOW()) WHERE id=?", id);
    assertFalse(lifecycle.closeExpiredOrdinaryOrder(id));
    assertEquals(
        2,
        jdbc.queryForObject("SELECT status FROM tb_voucher_order WHERE id=?", Integer.class, id));
    assertEquals(
        1L,
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM tb_business_outbox WHERE aggregate_id=? AND kind='ORDER_PAID'",
            Long.class,
            id));
  }

  @Test
  void repeatedTimeoutProducesOneClosureEventAndReconciliationCoversMissingMessage() {
    long id = order();
    jdbc.update("DELETE FROM tb_business_outbox WHERE aggregate_id=? AND kind='ORDER_EXPIRE'", id);
    jdbc.update(
        "UPDATE tb_voucher_order SET expire_at=TIMESTAMPADD(SECOND,-1,NOW()) WHERE id=?", id);
    maintenance.expireOrdinaryOrders();
    maintenance.expireOrdinaryOrders();
    assertEquals(
        4,
        jdbc.queryForObject("SELECT status FROM tb_voucher_order WHERE id=?", Integer.class, id));
    assertEquals(
        1L,
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM tb_business_outbox WHERE aggregate_id=? AND kind='ORDER_CLOSED'",
            Long.class,
            id));
    assertFalse(orders.payMockOrder(id, 100L).getSuccess());
  }

  @Test
  void earlyTimeoutReschedulesUsingCurrentDeadline() {
    long id = order();
    long expiry =
        jdbc.queryForObject(
            "SELECT id FROM tb_business_outbox WHERE aggregate_id=? AND kind='ORDER_EXPIRE'",
            Long.class,
            id);
    consumer.consume(event(expiry), "deadlines");
    consumer.consume(event(expiry), "deadlines");
    assertEquals(
        1,
        jdbc.queryForObject("SELECT status FROM tb_voucher_order WHERE id=?", Integer.class, id));
    assertEquals(
        2L,
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM tb_business_outbox WHERE aggregate_id=? AND kind='ORDER_EXPIRE'",
            Long.class,
            id));
  }

  @Test
  void paymentAndExpirationRaceHasOneTerminalStateAndOneEvent() throws Exception {
    long id = order();
    jdbc.update(
        "UPDATE tb_voucher_order SET expire_at=TIMESTAMPADD(SECOND,-1,NOW()) WHERE id=?", id);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<?> close = pool.submit(() -> lifecycle.closeExpiredOrdinaryOrder(id));
      Future<Result> pay =
          pool.submit(
              () -> {
                UserDTO user = new UserDTO();
                user.setId(USER);
                UserHolder.saveUser(user);
                try {
                  return orders.payMockOrder(id, 100L);
                } finally {
                  UserHolder.removeUser();
                }
              });
      close.get(10, TimeUnit.SECONDS);
      assertFalse(pay.get(10, TimeUnit.SECONDS).getSuccess());
      assertEquals(
          4,
          jdbc.queryForObject("SELECT status FROM tb_voucher_order WHERE id=?", Integer.class, id));
      assertEquals(
          1L,
          jdbc.queryForObject(
              "SELECT COUNT(*) FROM tb_business_outbox WHERE aggregate_id=? AND kind "
                  + "IN ('ORDER_PAID','ORDER_CLOSED')",
              Long.class,
              id));
    } finally {
      pool.shutdownNow();
    }
  }
}
