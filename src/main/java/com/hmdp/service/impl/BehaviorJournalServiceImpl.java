package com.hmdp.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.analytics.BehaviorEvent;
import com.hmdp.analytics.KafkaBehaviorTransport;
import com.hmdp.entity.BusinessOutboxTask;
import com.hmdp.mapper.BehaviorMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.messaging.DeliveryRetryPolicy;
import com.hmdp.service.IBehaviorJournalService;
import com.hmdp.utils.FailureMessages;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Resource;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class BehaviorJournalServiceImpl implements IBehaviorJournalService {

  @Resource
  private BehaviorMapper behaviorMapper;

  @Resource
  private VoucherOrderMapper voucherOrderMapper;

  private final ObjectMapper json;
  private final TransactionTemplate transactions;
  private final ObjectProvider<KafkaBehaviorTransport> transport;
  private final AtomicLong pending = new AtomicLong();
  private final AtomicLong review = new AtomicLong();

  @Value("${hmdp.analytics.enabled:false}")
  private boolean enabled;

  public BehaviorJournalServiceImpl(
      ObjectMapper json,
      TransactionTemplate transactions,
      ObjectProvider<KafkaBehaviorTransport> transport) {
    this.json = json;
    this.transactions = transactions;
    this.transport = transport;
  }

  @Autowired
  public void bindMetrics(MeterRegistry registry) {
    registry.gauge("hmdp.analytics.outbox.pending", pending);
    registry.gauge("hmdp.analytics.outbox.review", review);
  }

  /**
   * 从持久订单快照生成分析事件，在当前业务事务内写入分析 Outbox。
   *
   * <p>事件号沿用来源任务 ID，重放不会生成新的业务身份；当前券的价格或归属不替代历史快照。
   */
  @Override
  public void appendOrderBehaviorEvent(BusinessOutboxTask source) {
    if (!enabled) {
      return;
    }
    if (!TransactionSynchronizationManager.isActualTransactionActive()) {
      throw new IllegalStateException("订单分析事件需要业务事务");
    }
    List<Map<String, Object>> rows =
        voucherOrderMapper.selectBehaviorSnapshot(source.getAggregateId());
    if (rows.isEmpty() || rows.get(0).get("shop_id_snapshot") == null) {
      return;
    }
    Map<String, Object> order = rows.get(0);
    BehaviorEvent event =
        new BehaviorEvent(
            "order:" + source.getId(),
            ((Number) order.get("shop_id_snapshot")).longValue(),
            ((Number) order.get("user_id")).longValue(),
            "ORDER_PAID".equals(source.getKind()) ? "PAID" : "ORDER",
            source.getCreatedAt().getTime());
    try {
      behaviorMapper.insertOutboxEvent(
          event.getEventId(), event.getShopId(), json.writeValueAsString(event));
    } catch (JsonProcessingException bad) {
      throw new IllegalArgumentException("订单分析事件编码失败", bad);
    }
  }

  @Override
  public void recordShopVisit(long shopId, Long userId) {
    submitShopVisitEvent(shopId, userId);
  }

  /**
   * 提交一条访问统计事件并返回事件号，用于后续查询落库结果。
   *
   * <p>访问事件走尽力投递；返回事件号只表示发起采集，不保证 Kafka 已确认或数据库已落库。
   *
   * <p>分析功能未开启时返回 null。
   */
  @Override
  public String submitShopVisitEvent(long shopId, Long userId) {
    if (enabled) {
      String eventId = UUID.randomUUID().toString();
      transport
          .getObject()
          .publishVisitBestEffort(
              new BehaviorEvent(eventId, shopId, userId, "VISIT", System.currentTimeMillis()));
      return eventId;
    }
    return null;
  }

  @Scheduled(fixedDelay = 1000)
  /**
   * 领取并发送一批持久分析事件，网络发送在认领事务提交后进行。
   *
   * <p>只有 Kafka 确认后才写 SENT；确认丢失时沿用原事件号重试，由消费端幂等去重。
   */
  @Override
  public void relayAnalyticsOutboxBatch() {
    if (!enabled) {
      return;
    }
    String owner = UUID.randomUUID().toString();
    List<Map<String, Object>> rows =
        transactions.execute(
            tx -> {
              List<Map<String, Object>> batch = behaviorMapper.selectClaimableEvents();
              for (Map<String, Object> row : batch) {
                behaviorMapper.claimEvent(owner, (String) row.get("event_id"));
              }
              return batch;
            });
    for (Map<String, Object> row : rows) {
      try {
        if (behaviorMapper.renewLease((String) row.get("event_id"), owner) == 0) {
          continue;
        }
        transport
            .getObject()
            .publishAndAwaitAcknowledgement(
                json.readValue(row.get("payload").toString(), BehaviorEvent.class));
        behaviorMapper.markSent((String) row.get("event_id"), owner);
      } catch (Exception failure) {
        int attempts = ((Number) row.get("attempts")).intValue() + 1;
        behaviorMapper.recordSendFailure(
            attempts >= DeliveryRetryPolicy.MAX_SEND_ATTEMPTS ? "REVIEW" : "PENDING",
            attempts,
            FailureMessages.summarize(failure),
            DeliveryRetryPolicy.delaySeconds(attempts),
            (String) row.get("event_id"),
            owner);
      }
    }
  }

  @Scheduled(fixedDelay = 60000)
  @Override
  public void redriveMissingAnalyticsEvents() {
    if (!enabled) {
      return;
    }
    behaviorMapper.redriveMissingEvents();
  }

  @Scheduled(fixedDelay = 10000)
  @Override
  public void refreshMetrics() {
    if (!enabled) {
      return;
    }
    pending.set(behaviorMapper.countPendingEvents());
    review.set(behaviorMapper.countReviewEvents());
  }
}
