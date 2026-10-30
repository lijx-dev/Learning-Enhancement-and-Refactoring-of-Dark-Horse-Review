package com.hmdp.service.impl;

import com.hmdp.entity.BusinessOutboxTask;
import com.hmdp.enums.OutboxState;
import com.hmdp.mapper.BusinessOutboxMapper;
import com.hmdp.mapper.MessageMapper;
import com.hmdp.mapper.SeckillReservationMapper;
import com.hmdp.messaging.BusinessEvent;
import com.hmdp.messaging.BusinessEventType;
import com.hmdp.messaging.DeliveryRetryPolicy;
import com.hmdp.messaging.ProjectionResult;
import com.hmdp.service.IBusinessEventService;
import com.hmdp.service.IBusinessProjectionService;
import com.hmdp.utils.FailureMessages;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Resource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class BusinessEventServiceImpl implements IBusinessEventService {

  @Resource
  private BusinessOutboxMapper businessOutboxMapper;

  @Resource
  private MessageMapper messageMapper;

  @Resource
  private SeckillReservationMapper seckillReservationMapper;

  private final TransactionTemplate transactions;
  private final IBusinessProjectionService projector;
  private final MeterRegistry metrics;

  public BusinessEventServiceImpl(
      TransactionTemplate transactions,
      IBusinessProjectionService projector,
      MeterRegistry metrics) {
    this.transactions = new TransactionTemplate(transactions.getTransactionManager());
    this.transactions.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
    this.projector = projector;
    this.metrics = metrics;
  }

  public static String resolveConsumerGroup(String kind) {
    return BusinessEventType.from(kind).getConsumerGroup();
  }

  @Override
  public void consume(BusinessEvent event, String group) {
    if (event.getSchemaVersion() != 1
        || event.getEventId() <= 0
        || event.getKind() == null
        || !group.equals(resolveConsumerGroup(event.getKind()))) {
      throw new IllegalArgumentException("事件版本、事件号或消费组无效");
    }
    long started = System.nanoTime();
    try {
      transactions.execute(
          tx -> {
            consumeLockedTask(event, group);
            return null;
          });
    } finally {
      metrics
          .timer("hmdp.mq.consume", "group", group)
          .record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
    }
  }

  private void consumeLockedTask(BusinessEvent event, String group) {
    // 只锁当前事件；业务效果、Inbox 和任务完成状态仍在同一事务提交。
    BusinessOutboxTask task = businessOutboxMapper.selectEventForUpdate(event.getEventId());
    if (task == null) {
      throw new IllegalArgumentException("消息没有对应的持久化事件");
    }
    if (!event.getKind().equals(task.getKind())
        || event.getAggregateId() != task.getAggregateId()) {
      throw new IllegalArgumentException("消息内容与持久化事件不一致");
    }
    if (messageMapper.countInboxEvent(group, event.getEventId()) > 0) {
      businessOutboxMapper.markDuplicateDone(event.getEventId());
      metrics.counter("hmdp.mq.consumptions", "group", group, "result", "duplicate").increment();
      return;
    }
    if (OutboxState.REVIEW.matches(task.getState())) {
      throw new IllegalStateException("事件等待人工复核");
    }
    if (projector.project(task) == ProjectionResult.CONTINUE) {
      appendFeedContinuation(task);
    }
    messageMapper.insertInboxEvent(group, event.getEventId());
    businessOutboxMapper.markConsumed(event.getEventId());
    metrics.counter("hmdp.mq.consumptions", "group", group, "result", "success").increment();
  }

  private void appendFeedContinuation(BusinessOutboxTask task) {
    // 游标由当前批次推进；后续批次独立持久化，沿用原发布时间。
    long cursor = businessOutboxMapper.selectCursor(task.getId());
    businessOutboxMapper.insertFeedContinuation(
        task.getKind(), task.getAggregateId(), task.getUserId(), cursor, task.getCreatedAt());
  }

  /**
   * 保存消费失败并决定 Broker 是否可以确认当前消息。
   *
   * @return true 表示对应任务已经完成，或失败已持久隔离；false 表示仍需重新投递。 无效载荷按 Broker 消息身份隔离，不能借其 eventId 把其他真实任务标成
   *     REVIEW。
   */
  @Override
  public boolean recordConsumptionFailure(
      BusinessEvent event,
      String group,
      String brokerId,
      String payload,
      RuntimeException failure,
      int deliveries) {
    return Boolean.TRUE.equals(
        transactions.execute(
            tx -> {
              boolean valid = false;
              if (event != null && event.getEventId() > 0) {
                BusinessOutboxTask task =
                    businessOutboxMapper.selectEventForUpdate(event.getEventId());
                if (task != null) {
                  try {
                    valid =
                        event.getSchemaVersion() == 1
                            && group.equals(resolveConsumerGroup(event.getKind()))
                            && Objects.equals(event.getKind(), task.getKind())
                            && event.getAggregateId() == task.getAggregateId();
                  } catch (RuntimeException invalid) {
                    valid = false;
                  }
                  if (valid && OutboxState.DONE.matches(task.getState())) {
                    return true;
                  }
                  if (valid) {
                    businessOutboxMapper.recordConsumeFailure(
                        FailureMessages.summarize(failure), event.getEventId());
                  }
                }
              }
              if (valid && deliveries < DeliveryRetryPolicy.MAX_CONSUME_ATTEMPTS) {
                return false;
              }
              String key = valid ? "outbox:" + event.getEventId() : "broker:" + brokerId;
              messageMapper.insertFailure(
                  group,
                  key,
                  valid ? event.getEventId() : null,
                  brokerId,
                  payload,
                  FailureMessages.summarize(failure));
              if (valid) {
                businessOutboxMapper.markReview(event.getEventId());
              }
              if (valid && "SECKILL_FULFILL".equals(event.getKind())) {
                if (seckillReservationMapper.markProcessingForReview(
                        FailureMessages.summarize(failure), event.getAggregateId())
                    > 0) {
                  seckillReservationMapper.insertDeadLetter(
                      "mq:outbox:" + event.getEventId(),
                      event.getAggregateId(),
                      payload,
                      FailureMessages.summarize(failure));
                }
              }
              metrics
                  .counter("hmdp.mq.consumptions", "group", group, "result", "quarantined")
                  .increment();
              return true;
            }));
  }
}
