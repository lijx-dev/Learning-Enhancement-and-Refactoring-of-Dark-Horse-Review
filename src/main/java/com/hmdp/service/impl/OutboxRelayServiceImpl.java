package com.hmdp.service.impl;

import com.hmdp.entity.BusinessOutboxTask;
import com.hmdp.enums.OutboxState;
import com.hmdp.mapper.BusinessOutboxMapper;
import com.hmdp.mapper.SeckillReservationMapper;
import com.hmdp.messaging.BusinessEvent;
import com.hmdp.messaging.DeliveryRetryPolicy;
import com.hmdp.messaging.EventPublisher;
import com.hmdp.service.IBusinessEventService;
import com.hmdp.service.IOutboxRelayService;
import com.hmdp.utils.FailureMessages;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

@Slf4j
@Service
public class OutboxRelayServiceImpl implements IOutboxRelayService {

  @Resource
  private BusinessOutboxMapper businessOutboxMapper;

  @Resource
  private SeckillReservationMapper seckillReservationMapper;

  private final TransactionTemplate transactions;
  private final ObjectProvider<EventPublisher> publishers;
  private final IBusinessEventService consumer;
  private final MeterRegistry metrics;
  private final AtomicLong pending = new AtomicLong();
  private final AtomicLong review = new AtomicLong();
  private final AtomicLong inflight = new AtomicLong();

  @Value("${hmdp.mq.enabled:false}")
  private boolean mqEnabled;

  @Value("${hmdp.mq.lease-seconds:60}")
  private int leaseSeconds;

  @Value("${hmdp.mq.relay-batch-size:20}")
  private int batchSize;

  @Value("${hmdp.mq.redelivery-seconds:300}")
  private int redeliverySeconds;

  public OutboxRelayServiceImpl(
      TransactionTemplate transactions,
      ObjectProvider<EventPublisher> publishers,
      IBusinessEventService consumer,
      MeterRegistry metrics) {
    this.transactions = transactions;
    this.publishers = publishers;
    this.consumer = consumer;
    this.metrics = metrics;
    metrics.gauge("hmdp.outbox.pending", pending);
    metrics.gauge("hmdp.outbox.review", review);
    metrics.gauge("hmdp.outbox.inflight", inflight);
  }

  /**
   * 用短事务领取一批可投递任务，写入租约后提交。
   *
   * <p>SKIP LOCKED 允许其他 worker 领取不同任务；网络投递放在提交之后，避免长时间持有行锁。
   */
  @Override
  public List<BusinessOutboxTask> claimOutboxBatch(String leaseOwner) {
    return transactions.execute(
        tx -> {
          List<BusinessOutboxTask> tasks =
              businessOutboxMapper.selectClaimableEvents(Math.max(1, Math.min(100, batchSize)));
          for (BusinessOutboxTask task : tasks) {
            businessOutboxMapper.claimEvent(leaseOwner, effectiveLeaseSeconds(), task.getId());
          }
          return tasks;
        });
  }

  /**
   * 领取并投递一批 Outbox 任务，不保证一次清空所有待处理任务。
   *
   * <p>每批使用独立租约 owner，过期或重叠执行时通过 owner 条件避免覆盖新 worker 的结果。
   */
  @Override
  public void relayOutboxBatch() {
    // 批次独立 owner 防止同进程重叠调用在租约过期后互相覆盖。
    String leaseOwner = UUID.randomUUID().toString();
    for (BusinessOutboxTask task : claimOutboxBatch(leaseOwner)) {
      sendClaimedTask(task, leaseOwner);
    }
  }

  private void sendClaimedTask(BusinessOutboxTask task, String leaseOwner) {
    long started = System.nanoTime();
    try {
      if (businessOutboxMapper.renewLease(effectiveLeaseSeconds(), task.getId(), leaseOwner) == 0) {
        return;
      }
      if (deliverOutboxTask(task, leaseOwner)) {
        metrics.counter("hmdp.mq.sends", "result", "success").increment();
      }
    } catch (RuntimeException failure) {
      recordSendFailure(task, leaseOwner, failure);
      metrics.counter("hmdp.mq.sends", "result", "retry").increment();
      log.warn("事件 {} 投递失败，将按原事件号重试: {}", task.getId(), FailureMessages.summarize(failure));
    } finally {
      metrics.timer("hmdp.mq.send").record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
    }
  }

  private boolean deliverOutboxTask(BusinessOutboxTask task, String leaseOwner) {
    BusinessEvent event = BusinessEvent.from(task);
    if (mqEnabled) {
      publishers.getObject().publish(event);
      // 快速消费可能已经完成；owner/state 条件防止 DONE 被覆盖成 SENT。
      businessOutboxMapper.markSent(task.getId(), leaseOwner);
      return true;
    }
    if (event.getDeliverAt() != null && event.getDeliverAt() > System.currentTimeMillis()) {
      businessOutboxMapper.deferDeadline(task.getId(), leaseOwner);
      return false;
    }
    consumer.consume(event, BusinessEventServiceImpl.resolveConsumerGroup(event.getKind()));
    return true;
  }

  private void recordSendFailure(
      BusinessOutboxTask task, String leaseOwner, RuntimeException failure) {
    int attempts = task.getAttempts() + 1;
    boolean needsReview = attempts >= DeliveryRetryPolicy.MAX_SEND_ATTEMPTS;
    String reason = FailureMessages.summarize(failure);
    transactions.execute(
        tx -> {
          int changed =
              businessOutboxMapper.recordSendFailure(
                  attempts,
                  DeliveryRetryPolicy.delaySeconds(attempts),
                  needsReview ? OutboxState.REVIEW.name() : OutboxState.PENDING.name(),
                  reason,
                  task.getId(),
                  leaseOwner);
          if (changed > 0 && needsReview && "SECKILL_FULFILL".equals(task.getKind())) {
            seckillReservationMapper.markProcessingForReview(reason, task.getAggregateId());
          }
          return null;
        });
  }

  private int effectiveLeaseSeconds() {
    return Math.max(10, leaseSeconds);
  }

  @Scheduled(fixedDelay = 60000)
  /**
   * 重新排队已发送但超过观察窗口仍未消费的事件，沿用原事件号。
   *
   * <p>未来才到期的关单事件暂不重投；重投后的重复消费由 Inbox 与业务幂等规则处理。
   */
  @Override
  public void redriveUnconsumedEvents() {
    int age = Math.max(30, redeliverySeconds);
    // Future deadlines are expected to remain unconsumed. Redrive only after their due time.
    businessOutboxMapper.redriveUnconsumedEvents(-age, -age);
  }

  @Scheduled(fixedDelay = 10000)
  @Override
  public void refreshMetrics() {
    pending.set(businessOutboxMapper.countPendingEvents());
    review.set(businessOutboxMapper.countReviewEvents());
    inflight.set(businessOutboxMapper.countDueSentEvents());
  }
}
