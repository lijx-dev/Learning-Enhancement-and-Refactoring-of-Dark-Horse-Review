package com.hmdp.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.hmdp.dto.UserDTO;
import com.hmdp.mapper.BusinessOutboxMapper;
import com.hmdp.service.impl.BusinessOutboxServiceImpl;
import com.hmdp.service.impl.OutboxRelayServiceImpl;
import com.hmdp.service.impl.SeckillServiceImpl;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import com.hmdp.utils.UserHolder;
import java.util.Arrays;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest(
    properties = {
      "citylens.reliability.scheduling-enabled=false",
      "hmdp.mq.enabled=true",
      "hmdp.mq.seckill-enabled=true",
      "hmdp.mq.group-prefix=hmdp-integration"
    })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class RocketMqIntegrationTest {
  @Autowired JdbcTemplate jdbc;
  @Autowired BusinessOutboxMapper businessOutboxMapper;
  @Autowired TransactionTemplate transactions;
  @Autowired BusinessOutboxServiceImpl outbox;
  @Autowired OutboxRelayServiceImpl relay;
  @Autowired RocketMqTransport transport;
  @Autowired VoucherOrderServiceImpl orders;
  @Autowired SeckillServiceImpl coordinator;
  @Autowired StringRedisTemplate redis;
  private static final long SKU = 916001;
  private static final long USER = 916002;

  @BeforeEach
  void isolate() {
    assertEquals("hmdp_optimization_test", jdbc.queryForObject("SELECT DATABASE()", String.class));
    jdbc.update("DELETE FROM tb_voucher_order WHERE voucher_id=?", SKU);
    jdbc.update("DELETE FROM tb_voucher WHERE id=?", SKU);
    jdbc.update(
        "INSERT INTO "
            + "tb_voucher(id,shop_id,title,rules,pay_value,actual_value,type,status) "
            + "VALUES(?,1,'rocket-test','rules',100,200,0,1)",
        SKU);
    UserDTO user = new UserDTO();
    user.setId(USER);
    UserHolder.saveUser(user);
  }

  @AfterEach
  void clear() {
    UserHolder.removeUser();
  }

  private static void await(BooleanSupplier condition) throws InterruptedException {
    long until = System.currentTimeMillis() + 45000;
    while (System.currentTimeMillis() < until) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(100);
    }
    fail("真实 RocketMQ 消费未在 45 秒内完成");
  }

  @Test
  void realBrokerAcceptsAndDeduplicatesRepeatedDelivery() throws Exception {
    transactions.execute(
        tx -> {
          outbox.appendBusinessEvent("ORDER_CREATED", 916003, USER);
          return null;
        });
    long id = jdbc.queryForObject("SELECT MAX(id) FROM tb_business_outbox", Long.class);
    BusinessEvent event = BusinessEvent.from(businessOutboxMapper.selectEventById(id));
    for (int i = 0; i < 5; i++) {
      transport.publish(event);
    }
    await(
        () ->
            jdbc.queryForObject(
                    "SELECT COUNT(*) FROM tb_message_inbox WHERE event_id=? AND "
                        + "consumer_group='orders'",
                    Long.class,
                    id)
                == 1);
    assertEquals(
        1L,
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM tb_order_event_history WHERE event_id=?", Long.class, id));
    assertEquals(
        "DONE",
        jdbc.queryForObject("SELECT state FROM tb_business_outbox WHERE id=?", String.class, id));
  }

  @Test
  void realTimerClosesUnpaidOrderAndPreservesPaidOrder() throws Exception {
    long unpaid = ((Number) orders.orderOrdinaryVoucher(SKU).getData()).longValue();
    long deadline =
        jdbc.queryForObject(
            "SELECT id FROM tb_business_outbox WHERE aggregate_id=? AND kind='ORDER_EXPIRE'",
            Long.class,
            unpaid);
    jdbc.update(
        "UPDATE tb_voucher_order SET expire_at=TIMESTAMPADD(SECOND,4,NOW(3)) WHERE id=?", unpaid);
    jdbc.update(
        "UPDATE tb_business_outbox SET deliver_at=(SELECT expire_at FROM "
            + "tb_voucher_order WHERE id=?) WHERE id=?",
        unpaid,
        deadline);
    relay.relayOutboxBatch();
    assertEquals(
        1,
        jdbc.queryForObject(
            "SELECT status FROM tb_voucher_order WHERE id=?", Integer.class, unpaid));
    await(
        () ->
            jdbc.queryForObject(
                    "SELECT status FROM tb_voucher_order WHERE id=?", Integer.class, unpaid)
                == 4);
    long paid = ((Number) orders.orderOrdinaryVoucher(SKU).getData()).longValue();
    assertNotEquals(unpaid, paid);
    long paidDeadline =
        jdbc.queryForObject(
            "SELECT id FROM tb_business_outbox WHERE aggregate_id=? AND kind='ORDER_EXPIRE'",
            Long.class,
            paid);
    jdbc.update(
        "UPDATE tb_voucher_order SET expire_at=TIMESTAMPADD(SECOND,4,NOW(3)) WHERE id=?", paid);
    jdbc.update(
        "UPDATE tb_business_outbox SET deliver_at=(SELECT expire_at FROM "
            + "tb_voucher_order WHERE id=?) WHERE id=?",
        paid,
        paidDeadline);
    assertTrue(orders.payMockOrder(paid, 100L).getSuccess());
    relay.relayOutboxBatch();
    await(
        () ->
            "DONE"
                .equals(
                    jdbc.queryForObject(
                        "SELECT state FROM tb_business_outbox WHERE id=?",
                        String.class,
                        paidDeadline)));
    assertEquals(
        2,
        jdbc.queryForObject("SELECT status FROM tb_voucher_order WHERE id=?", Integer.class, paid));
  }

  @Test
  void luaStreamHandoffAndRocketFulfillmentRemainIdempotent() throws Exception {
    long sku = 916010;
    jdbc.update("DELETE FROM tb_seckill_reservation WHERE voucher_id=?", sku);
    jdbc.update("DELETE FROM tb_voucher_order WHERE voucher_id=?", sku);
    jdbc.update("DELETE FROM tb_seckill_activity WHERE voucher_id=?", sku);
    jdbc.update("DELETE FROM tb_seckill_voucher WHERE voucher_id=?", sku);
    jdbc.update("DELETE FROM tb_voucher WHERE id=?", sku);
    jdbc.update(
        "INSERT INTO "
            + "tb_voucher(id,shop_id,title,rules,pay_value,actual_value,type,status) "
            + "VALUES(?,1,'mq-seckill','rules',0,100,1,1)",
        sku);
    jdbc.update(
        "INSERT INTO tb_seckill_voucher(voucher_id,stock,begin_time,end_time) "
            + "VALUES(?,1,TIMESTAMPADD(HOUR,-1,NOW()),TIMESTAMPADD(DAY,1,NOW()))",
        sku);
    jdbc.update("INSERT INTO tb_seckill_activity(voucher_id,total_stock) VALUES(?,1)", sku);
    redis.delete(
        Arrays.asList(
            "seckill:stock:" + sku,
            "seckill:order:" + sku,
            "seckill:requests:" + sku,
            "seckill:activity:" + sku));
    assertTrue(coordinator.publishActivity(sku).getSuccess());
    long order = ((Number) orders.seckillVoucher(sku).getData()).longValue();
    List<MapRecord<String, Object, Object>> records =
        redis
            .opsForStream()
            .read(
                Consumer.from("g1", "mq-test"),
                StreamReadOptions.empty().count(100),
                StreamOffset.create("stream.orders", ReadOffset.lastConsumed()));
    MapRecord<String, Object, Object> record =
        records.stream()
            .filter(r -> String.valueOf(order).equals(String.valueOf(r.getValue().get("id"))))
            .findFirst()
            .orElseThrow(AssertionError::new);
    ReflectionTestUtils.invokeMethod(coordinator, "handleOrderStreamMessage", record);
    ReflectionTestUtils.invokeMethod(coordinator, "handleOrderStreamMessage", record);
    assertEquals("PROCESSING", coordinator.queryReservationState(order));
    assertEquals(
        0L,
        jdbc.queryForObject("SELECT COUNT(*) FROM tb_voucher_order WHERE id=?", Long.class, order));
    assertEquals(
        1L,
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM tb_business_outbox WHERE idempotency_key=?",
            Long.class,
            "seckill:" + order));
    relay.relayOutboxBatch();
    await(() -> "SUCCESS".equals(coordinator.queryReservationState(order)));
    long event =
        jdbc.queryForObject(
            "SELECT id FROM tb_business_outbox WHERE idempotency_key=?",
            Long.class,
            "seckill:" + order);
    transport.publish(BusinessEvent.from(businessOutboxMapper.selectEventById(event)));
    assertEquals(
        1L,
        jdbc.queryForObject("SELECT COUNT(*) FROM tb_voucher_order WHERE id=?", Long.class, order));
    assertEquals(
        0,
        jdbc.queryForObject(
            "SELECT stock FROM tb_seckill_voucher WHERE voucher_id=?", Integer.class, sku));
    assertEquals("0", redis.opsForValue().get("seckill:stock:" + sku));
  }
}
