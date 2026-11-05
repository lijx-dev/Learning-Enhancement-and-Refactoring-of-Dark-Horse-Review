package com.hmdp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.dto.UserDTO;
import com.hmdp.mapper.BusinessOutboxMapper;
import com.hmdp.service.impl.BusinessOutboxServiceImpl;
import com.hmdp.service.impl.BusinessProjectionServiceImpl;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import com.hmdp.utils.UserHolder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

/** Real payment, Outbox, projection and UTC SQL export; all business writes roll back. */
@SpringBootTest(
    properties = {
      "citylens.reliability.scheduling-enabled=false", "citylens.payment.mock-enabled=true"
    })
@Transactional
class PaymentClockIntegrationTest {
  private static final long ORDER = 9007199254741999L;
  private static final long VOUCHER = 980010L;
  @Autowired JdbcTemplate jdbc;
  @Autowired VoucherOrderServiceImpl orders;
  @Autowired BusinessOutboxMapper mapper;
  @Autowired BusinessOutboxServiceImpl outbox;
  @Autowired BusinessProjectionServiceImpl projection;
  @Autowired ObjectMapper json;

  @Test
  void paymentAndProjectedEventShareOneInstantDespiteJvmTimezone() throws Exception {
    assertEquals("hmdp_optimization_test", jdbc.queryForObject("SELECT DATABASE()", String.class));
    jdbc.update("INSERT INTO tb_voucher(id,shop_id,title,pay_value,actual_value,type,status) "
        + "VALUES(?,1,'clock-test',100,100,0,1)", VOUCHER);
    jdbc.update("INSERT INTO tb_voucher_order(id,user_id,voucher_id,status,pay_value_snapshot,"
        + "shop_id_snapshot,voucher_type_snapshot,expire_at) "
        + "VALUES(?,1010,?,1,100,1,0,TIMESTAMPADD(MINUTE,10,NOW(3)))", ORDER, VOUCHER);
    UserDTO user = new UserDTO();
    user.setId(1010L);
    UserHolder.saveUser(user);
    try {
      assertTrue(orders.payMockOrder(ORDER, 100L).getSuccess());
      assertTrue(orders.payMockOrder(ORDER, 100L).getSuccess());
      assertEquals(1L, jdbc.queryForObject("SELECT COUNT(*) FROM tb_business_outbox "
          + "WHERE kind='ORDER_PAID' AND aggregate_id=?", Long.class, ORDER));
      assertEquals(0L, jdbc.queryForObject(
          "SELECT TIMESTAMPDIFF(MICROSECOND,o.pay_time,b.created_at) "
          + "FROM tb_voucher_order o JOIN tb_business_outbox b ON b.aggregate_id=o.id "
          + "WHERE o.id=? AND b.kind='ORDER_PAID'", Long.class, ORDER));
      assertEquals(1L, jdbc.queryForObject("SELECT COUNT(*) FROM tb_business_outbox "
          + "WHERE aggregate_id=? AND enqueued_at>=created_at", Long.class, ORDER));
      long event = jdbc.queryForObject("SELECT id FROM tb_business_outbox "
          + "WHERE kind='ORDER_PAID' AND aggregate_id=?", Long.class, ORDER);
      projection.project(mapper.selectEventById(event));
      assertEquals(0L, jdbc.queryForObject(
          "SELECT TIMESTAMPDIFF(MICROSECOND,o.pay_time,h.occurred_at) "
          + "FROM tb_voucher_order o JOIN tb_order_event_history h ON h.order_id=o.id "
          + "WHERE o.id=?", Long.class, ORDER));
      // Missing paid-order metadata must fail rather than silently omit the event.
      assertThrows(IllegalStateException.class,
          () -> outbox.appendBusinessEvent("ORDER_PAID", ORDER + 1, 1010L));
      exportUtcSource();
    } finally {
      UserHolder.removeUser();
    }
  }

  @SuppressWarnings("unchecked")
  private void exportUtcSource() throws Exception {
    Map<String, Object> contract = json.readValue(
        Paths.get("data-platform/contracts/source-contract.json").toFile(), Map.class);
    Map<String, List<String>> tables = (Map<String, List<String>>) contract.get("source_tables");
    Map<String, Object> evidence = new LinkedHashMap<>();
    Map<String, Object> sources = new LinkedHashMap<>();
    evidence.put("jvm_timezone", TimeZone.getDefault().getID());
    evidence.put("database", "hmdp_optimization_test");
    // Format TIMESTAMP in the UTC session as text, so JDBC cannot reinterpret it.
    jdbc.execute("SET SESSION time_zone='+00:00'");
    try {
      evidence.put("scenario_as_of", jdbc.queryForObject(
          "SELECT DATE_FORMAT(UTC_TIMESTAMP(6),'%Y-%m-%dT%H:%i:%s.%fZ')", String.class));
      for (Map.Entry<String, List<String>> entry : tables.entrySet()) {
        String table = entry.getKey();
        StringBuilder selected = new StringBuilder();
        for (String column : entry.getValue()) {
          if (selected.length() > 0) {
            selected.append(',');
          }
          String type = jdbc.queryForObject("SELECT DATA_TYPE FROM information_schema.columns "
              + "WHERE table_schema=DATABASE() AND table_name=? AND column_name=?",
              String.class, table, column);
          selected.append("timestamp".equals(type)
              ? "DATE_FORMAT(`" + column + "`,'%Y-%m-%dT%H:%i:%s.%fZ')" : "`" + column + "`");
          selected.append(" AS `").append(column).append('`');
        }
        String filter;
        if ("tb_voucher_order".equals(table)) {
          filter = "id=" + ORDER;
        } else if ("tb_order_event_history".equals(table)) {
          filter = "order_id=" + ORDER;
        } else if ("tb_business_outbox".equals(table)) {
          filter = "aggregate_id=" + ORDER + " AND kind='ORDER_PAID'";
        } else {
          filter = "id=" + ("tb_voucher".equals(table) ? VOUCHER : 1);
        }
        List<Map<String, String>> rows = jdbc.query(
            "SELECT " + selected + " FROM `" + table + "` WHERE " + filter,
            (rs, index) -> {
              Map<String, String> row = new LinkedHashMap<>();
              for (String column : entry.getValue()) {
                row.put(column, rs.getString(column));
              }
              return row;
            });
        sources.put(table, rows);
      }
      evidence.put("source_tables", sources);
      Path output = Paths.get(System.getProperty("payment.clock.evidence",
          "target/warehouse-payment-clock/source.json"));
      Files.createDirectories(output.getParent());
      json.writerWithDefaultPrettyPrinter().writeValue(output.toFile(), evidence);
    } finally {
      jdbc.execute("SET SESSION time_zone='+08:00'");
    }
  }
}
