# 10 Kafka 行为分析与热榜

[返回目录](README.md) · [上一章](09-RocketMQ可靠消息.md) · [下一章：社区与页面](11-社区搜索与页面验证.md)

**本章学习目标**：先区分浏览与订单分析的可靠性要求，再沿一条 eventId 阅读生产、消费、事务与 offset。最后手算 PV、登录 UV 和热度。读完应能说出回执、统计窗口与热榜缓存分别证明什么。

**本章导航**

- [1. 为什么需要独立行为链路](#section-01)
- [2. PV、UV 与当前统计口径](#section-02)
- [3. 为什么浏览与订单采用不同可靠性](#section-03)
- [4. 分区、消费组和 offset](#section-04)
- [5. 消费端怎么防止重复计数](#section-05)
  - [怎样读重复事件处理](#section-05-01)
- [6. 分钟桶为什么比每次扫全日志轻](#section-06)
- [7. 热榜怎么对外读取](#section-07)
- [8. 从同步计数推导事件日志与聚合](#section-08)
  - [一次打开店铺到底产生什么](#section-08-01)
  - [生产者幂等和业务去重不是同一层](#section-08-02)
  - [为什么 VISIT 不在请求线程等待发送](#section-08-03)
  - [订单行为为什么从另一个 Outbox 来](#section-08-04)
- [9. 用一个分区的 offset 演算消费与重放](#section-09)
- [10. 去重事实与桶计数必须同事务](#section-10)
- [11. 分钟窗口与 UV 的数字例子](#section-11)
  - [快照与事实的区别再看一次](#section-11-01)
- [12. 如何验证重放没有重复计数](#section-12)
- [13. 分析 Outbox 的发送确认为什么还不够](#section-13)
- [14. 体验页怎样提供可信证据](#section-14)
- [15. 扩展方向与边界](#section-15)
- [16. 从原始事件手算 PV、UV、窗口与热度](#section-16)
  - [先准备五条事件，再计算答案](#section-16-01)
  - [eventId 去重与用户去重分别在回答什么](#section-16-02)
  - [五分钟窗口并非逐毫秒滑动的最近 300 秒](#section-16-03)
  - [数据库提交与 offset 确认的先后推演](#section-16-04)
  - [浏览事件和订单事件为什么不用同一种投递保证](#section-16-05)
  - [热榜快照为什么落库后还会慢一点变化](#section-16-06)
  - [一次可信的页面实验该留下什么](#section-16-07)

<a id="section-01"></a>

## 1. 为什么需要独立行为链路

查店铺、创建订单、模拟支付会产生行为事件。直接在每次网页请求里同步更新复杂排行榜，会增加请求等待，把分析故障传给业务页面。

当前通过 Kafka 收集事件，消费端保存去重事实并聚合，页面读取短期热榜。RocketMQ 更偏可靠业务任务；Kafka 在这里承担可重放的行为日志与分析通道。不是因为“Kafka 比 RocketMQ 一定更快”才选，也不要求每个项目同时使用两种 MQ。

代码入口：[BehaviorJournalServiceImpl](../src/main/java/com/hmdp/service/impl/BehaviorJournalServiceImpl.java)、[KafkaBehaviorTransport](../src/main/java/com/hmdp/analytics/KafkaBehaviorTransport.java)、[BehaviorAggregationServiceImpl](../src/main/java/com/hmdp/service/impl/BehaviorAggregationServiceImpl.java)、[ShopHeatServiceImpl](../src/main/java/com/hmdp/service/impl/ShopHeatServiceImpl.java)。

<a id="section-02"></a>

## 2. PV、UV 与当前统计口径

PV 是访问事件次数；UV 是去重用户数。相同口径下，一个用户访问多次只增加一次 UV，但会产生多次 PV，所以 PV 通常不小于 UV。

当前实现：成功消费落库的 VISIT 事件增加 PV；VISIT 带有已登录 userId 时，按窗口对用户去重形成 logged UV。匿名事件没有登录身份，计 PV 不计 logged UV。因此匿名访客点击三次，PV 可以增加 3、UV 仍是 0，这是正确现象。

课程 HyperLogLog 用少量内存估算去重规模，适合近似统计。但本项目当前热榜采用 MySQL 事实、分钟桶和登录用户去重，**没有使用 HLL 计算匿名访客 UV**。浏览器刷新、测试点击和真实自然用户访问也尚未完整做反作弊区分。

<a id="section-03"></a>

## 3. 为什么浏览与订单采用不同可靠性

| 行为 | 当前方式 | 取舍 |
| --- | --- | --- |
| 浏览 VISIT | 有界内存队列，异步 Kafka 发送 | 不阻塞店铺阅读，拥塞/故障允许丢弃，指标可见 |
| 订单/模拟支付 | 数据库行为 Outbox | 与业务投影事务登记后续分析任务，可重试 |

浏览发送队列容量约 512，生产者缓冲也有界。Kafka 元数据等待和发送放到后台，避免读取页面因 Broker 暂时不可用一直卡住。队列满、发送失败可能丢事件，这是选择，不是 Bug 被隐藏。

订单事实不能直接照搬浏览的 best-effort。`ORDER_CREATED`、`ORDER_PAID` 业务处理时，在数据库事务内写分析 Outbox，事件 ID 类似 `order:{业务OutboxId}`，重试沿用稳定 ID。订单创建成功与热榜立即加一之间仍有异步延迟。

<a id="section-04"></a>

## 4. 分区、消费组和 offset

Topic 可以有多个分区，每个分区是一条有序日志；同一消费组分配分区给组内消费者。offset 是分区中的读取位置，不是业务 ID。不同分区之间不保证全局先后次序。

当前 Topic 为 `HmdpShopBehavior`，完整本地环境使用 3 分区、单副本，保留约 72 小时；默认消费组 `hmdp-heat-v1`。一个消费组扩实例能增加分区并行处理，但有效并行度受分区数限制。

消费者关闭自动提交，先处理数据库效果，再 `commitSync` 提交 offset。要分清：Kafka Producer 收到发送确认，并不表示数据库消费已经完成。

<a id="section-05"></a>

## 5. 消费端怎么防止重复计数

数据库事务内依次完成：

1. 按 eventId 登记 `tb_behavior_event`。
2. 只有新事件才更新对应商家与分钟的热度桶。
3. 登录访问登记去重用户。
4. 提交数据库，之后提交 Kafka offset。

数据库成功、offset 提交失败时，消息可能重读，但 eventId 主键会挡住第二次统计。这是 Kafka 至少一次读取与数据库幂等效果的组合，不是 Kafka 跨数据库事务的 Exactly Once。

无效消息按 topic/partition/offset 保存到失败表，再按实现的失败分类处理；数据库暂时异常不能被当作无效消息跳过。批次失败会把各分区位置回退到本批最早取到的 offset，避免只重试失败记录却跳过前面未提交效果。

<a id="section-05-01"></a>

### 怎样读重复事件处理

当前聚合服务先调用 Mapper 的 `insertEvent`。插入返回 0 时，再查询已有 eventId 的内容，核对 shopId、action、userId 与 occurredAt 全部一致：一致才视为重复；不一致会抛出“同一行为事件号对应不同内容”。

这比“任何 INSERT IGNORE 都当重复成功”更严谨。去重键相同既可能是真重试，也可能是事件身份冲突，必须分清。新事件插入和统计加一在同一数据库事务中，所以统计更新失败时不能留下一个阻止重试的已处理事实。

<a id="section-06"></a>

## 6. 分钟桶为什么比每次扫全日志轻

把每条事件的 occurredAt 切到分钟边界，按 `(shopId,bucketMs)` 汇总访问、订单和模拟支付。查询最近 5 分钟或 60 分钟时，读取相应分钟桶，而不是每次扫描全部历史事件。

当前窗口以分钟边界定义，是分钟粒度窗口，不是连续毫秒滑窗。登录 UV 则对整个窗口用户去重，不能把各分钟 UV 相加：同一用户跨两分钟访问，同样只能算窗口中的一个用户。

热度公式为：

```text
heat = PV + loggedUV × 2 + orders × 5 + mockPayments × 10
```

权重是演示规则，不是训练得到的推荐模型，也不是收入排序。模拟支付数量不能用来宣传真实成交额。

<a id="section-07"></a>

## 7. 热榜怎么对外读取

`GET /shop/hot-ranking?windowMinutes=5&limit=10`；窗口只支持 5 或 60，limit 1～100。后台约每 5 秒刷新 Redis `analytics:heat:5`/`analytics:heat:60`，快照约 15 秒过期。

Redis 快照丢失时，可以从数据库聚合重建；这使热榜副本比原始浏览入口更容易恢复。但它只恢复已经持久化的事实，不能把此前发送队列丢失的 VISIT 补回来。

接口有延迟是正常的：发送、消费、数据库提交、快照刷新都需要时间。看到点击后榜单没立即变化，不一定代表 Kafka 失败。

<a id="section-08"></a>

## 8. 从同步计数推导事件日志与聚合

<a id="section-08-01"></a>

### 一次打开店铺到底产生什么

正常店铺详情查询在成功后提交 VISIT；体验页缓存读调用纯查询方法，不为了缓存采样额外假造访问。点击“产生 3 次浏览”才显式产生三条演示 VISIT。

行为事件表示“在某个时间，某个店铺发生了一次动作”。统计系统不需要完整店铺实体，只需要业务身份和口径字段。

以下为 [BehaviorEvent.java](../src/main/java/com/hmdp/analytics/BehaviorEvent.java) 的相关源码片段。为便于阅读只展示当前部分，依赖与 import 请查看原文件；无需把片段另建类复制运行。

```java
package com.hmdp.analytics;

import lombok.Data;

@Data
public class BehaviorEvent {
  private int schemaVersion = 1;
  private String eventId;
  private long shopId;
  private Long userId;
  private String action;
  /** 事件发生时间，Unix 毫秒；保留 occurredAt 作为 JSON 协议字段。 */
  private long occurredAt;

  public BehaviorEvent() {}

  public BehaviorEvent(
      String eventId, long shopId, Long userId, String action, long occurredAtMillis) {
    this.eventId = eventId;
    this.shopId = shopId;
    this.userId = userId;
    this.action = action;
    this.occurredAt = occurredAtMillis;
  }

  public void validate() {
    if (schemaVersion != 1
        || eventId == null
        || eventId.isEmpty()
        || eventId.length() > 64
        || shopId <= 0
        || (userId != null && userId <= 0)
        || occurredAt <= 0
        || occurredAt > System.currentTimeMillis() + 60000
        || !("VISIT".equals(action) || "ORDER".equals(action) || "PAID".equals(action))) {
      throw new IllegalArgumentException("行为事件格式无效");
    }
  }
}
```


schemaVersion 表示消息格式版本，eventId 用来去重，shopId 确定归属，userId 可空用于匿名访问，action 仅允许 VISIT/ORDER/PAID，occurredAt 为事件发生毫秒时间。

当前 validate 拒绝无效身份、过长事件号、非支持动作和过于未来的时间。它不验证用户实际有没有访问过店铺，也不提供完整反作弊；真实来源约束与采集口径仍需要业务层管理。

<a id="section-08-02"></a>

### 生产者幂等和业务去重不是同一层

当前 Kafka 生产者开启 enable.idempotence，acks=all。它帮助处理 Producer 层重试产生的重复，但不能阻止应用把同一业务动作换两个 eventId 发送。消费端仍要按稳定逻辑事件去重。

本地 Topic 只有单副本，acks=all 不是凭空出现多机复制。生产高可用还依赖副本、ISR 配置、存储与故障恢复，不把客户端参数当作完整容灾方案。

<a id="section-08-03"></a>

### 为什么 VISIT 不在请求线程等待发送

以下为 [KafkaBehaviorTransport.java](../src/main/java/com/hmdp/analytics/KafkaBehaviorTransport.java) 的相关源码片段。为便于阅读只展示当前部分，依赖与 import 请查看原文件；无需把片段另建类复制运行。

```java
/**
 * Visit statistics tolerate loss on process crashes; durable order events use the analytics
 * outbox.
 */
public void publishVisitBestEffort(BehaviorEvent event) {
  try {
    visits.execute(
        () -> {
          try {
            producer.send(
                buildProducerRecord(event),
                (metadata, failure) ->
                    metrics
                        .counter(
                            "hmdp.kafka.visits", "result", failure == null ? "sent" : "dropped")
                        .increment());
          } catch (Exception unavailable) {
            metrics.counter("hmdp.kafka.visits", "result", "dropped").increment();
          }
        });
  } catch (RejectedExecutionException unavailable) {
    metrics.counter("hmdp.kafka.visits", "result", "dropped").increment();
  }
}
```


外层提交到 visits 的有界 executor，内层才调用 Kafka send；拒绝入队和发送异常都统计 dropped。店铺请求不在这里等待 Kafka 网络完成。

返回了 eventId 表示已经创建一次采集身份，不保证进队、发送、消费都成功。因此体验页会继续查持久回执，并允许长期未确认，不把“有 UUID”当“统计一定入库”。

<a id="section-08-04"></a>

### 订单行为为什么从另一个 Outbox 来

以下为 [BehaviorJournalServiceImpl.java](../src/main/java/com/hmdp/service/impl/BehaviorJournalServiceImpl.java) 的相关源码片段。为便于阅读只展示当前部分，依赖与 import 请查看原文件；无需把片段另建类复制运行。

```java
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
```


这里接收的是业务 ORDER_CREATED/ORDER_PAID 投影任务，从订单读取店铺与用户快照，生成 `order:{业务eventId}`，把编码后的事件写入 `tb_behavior_outbox`。登记发生在业务消息消费的数据库事务内。

可靠分析链路是“订单事实 → 业务 Outbox → 业务消息消费 → 分析 Outbox → Kafka → 去重事实”，不是简单在浏览器建单按钮后顺手 send。每个交接点都要保留稳定身份与恢复责任。

**当前边界**：普通建单调用 lifecycle.created；现有秒杀 createVoucherOrder 没有同样调用 ORDER_CREATED 生命周期登记。因此不能承诺热榜已统计所有秒杀创建订单。免费券 status=2 也不表示发生了一次模拟支付 PAID。后续统一事件登记是明确的优化方向。

<a id="section-09"></a>

## 9. 用一个分区的 offset 演算消费与重放

假设本次 poll 取得分区 P0 的 offset 20、21、22。offset 表示日志位置，已提交消费位置常表示下一条要读取的位置，不是“最后生成的订单号”。

先处理 20 成功，21 因数据库暂时不可用失败。如果这时提交本批末尾位置，重启可能跳过未处理的 21/22。当前批次异常不提交，seek 回本批各分区的最早记录；20 再次读到，由 eventId 去重，不加第二次统计。

如果同时有 P1 的记录，要记住每个分区自己的 rewind 位置，不用 P0 的数值管理 P1。跨分区也没有总顺序可背。

以下为 [KafkaBehaviorTransport.java](../src/main/java/com/hmdp/analytics/KafkaBehaviorTransport.java) 的相关源码片段。为便于阅读只展示当前部分，依赖与 import 请查看原文件；无需把片段另建类复制运行。

```java
private void consumeBehaviorEvents() {
  Properties p = new Properties();
  p.put("bootstrap.servers", servers);
  p.put("group.id", group);
  p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  p.put("enable.auto.commit", "false");
  p.put("auto.offset.reset", "earliest");
  p.put("max.poll.records", "50");
  p.put("max.poll.interval.ms", "300000");
  try (KafkaConsumer<String, String> client = new KafkaConsumer<>(p)) {
    consumer = client;
    client.subscribe(Collections.singleton(topic));
    consuming.set(1);
    while (running) {
      ConsumerRecords<String, String> batch = client.poll(Duration.ofSeconds(1));
      client
          .metrics()
          .forEach(
              (name, metric) -> {
                if ("records-lag-max".equals(name.name())
                    && metric.metricValue() instanceof Number) {
                  double value = ((Number) metric.metricValue()).doubleValue();
                  if (Double.isFinite(value) && value >= 0) {
                    lag.set((long) value);
                  }
                }
              });
      Map<TopicPartition, Long> rewind = new HashMap<>();
      for (TopicPartition partition : batch.partitions()) {
        rewind.put(partition, batch.records(partition).get(0).offset());
      }
      try {
        for (ConsumerRecord<String, String> record : batch) {
          BehaviorEvent event;
          try {
            event = json.readValue(record.value(), BehaviorEvent.class);
            event.validate();
          } catch (Exception malformed) {
            quarantine(record, malformed);
            continue;
          }
          try {
            aggregation.accept(event);
          } catch (IllegalArgumentException conflict) {
            quarantine(record, conflict);
            continue;
          }
          metrics.counter("hmdp.kafka.consumptions", "result", "success").increment();
          metrics
              .timer("hmdp.kafka.event.delay")
              .record(
                  Math.max(0, System.currentTimeMillis() - event.getOccurredAt()),
                  TimeUnit.MILLISECONDS);
        }
        if (!batch.isEmpty()) {
          client.commitSync();
        } // Commit only after all database effects commit.
      } catch (RuntimeException unavailable) {
        metrics.counter("hmdp.kafka.consumptions", "result", "retry").increment();
        Set<TopicPartition> assigned = client.assignment();
        rewind.forEach(
            (partition, offset) -> {
              if (assigned.contains(partition)) {
                client.seek(partition, offset);
              }
            });
        log.warn("Kafka 统计批次失败，将按原事件号重放: {}", FailureMessages.summarize(unavailable));
        try {
          Thread.sleep(500);
        } catch (InterruptedException stopped) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  } catch (WakeupException stopping) {
    if (running) {
      log.warn("Kafka 消费被意外唤醒", stopping);
    }
  } catch (RuntimeException failure) {
    log.error("Kafka 行为消费者退出", failure);
  } finally {
    consuming.set(0);
    consumer = null;
  }
}
```


把长方法拆成六块：配置手动提交与消费组；poll 小批；采样 lag；记录各分区 rewind；逐条解析/验证/聚合；成功 commitSync、异常 seek。

格式错误或逻辑身份冲突持久隔离后可以继续；数据库不可用走整批重试。如果隔离记录本身无法保存，也必须保留重放责任。不能把所有异常 catch 后 continue，否则暂时数据库故障会变成静默丢失。

<a id="section-10"></a>

## 10. 去重事实与桶计数必须同事务

以下为 [BehaviorAggregationServiceImpl.java](../src/main/java/com/hmdp/service/impl/BehaviorAggregationServiceImpl.java) 的相关源码片段。为便于阅读只展示当前部分，依赖与 import 请查看原文件；无需把片段另建类复制运行。

```java
public void accept(BehaviorEvent event) {
  event.validate();
  transactions.execute(
      tx -> {
        long bucket = event.getOccurredAt() / 60000 * 60000;
        if (behaviorMapper.insertEvent(
                event.getEventId(),
                event.getShopId(),
                event.getUserId(),
                event.getAction(),
                event.getOccurredAt(),
                bucket)
            == 0) {
          Map<String, Object> old = behaviorMapper.selectEvent(event.getEventId());
          if (((Number) old.get("shop_id")).longValue() != event.getShopId()
              || !Objects.equals(old.get("action"), event.getAction())
              || !Objects.equals(old.get("user_id"), event.getUserId())
              || ((Number) old.get("occurred_at_ms")).longValue() != event.getOccurredAt()) {
            throw new IllegalArgumentException("同一行为事件号对应不同内容");
          }
          return null;
        }
        behaviorMapper.accumulateBucket(
            event.getShopId(),
            bucket,
            "VISIT".equals(event.getAction()) ? 1 : 0,
            "ORDER".equals(event.getAction()) ? 1 : 0,
            "PAID".equals(event.getAction()) ? 1 : 0);
        if ("VISIT".equals(event.getAction()) && event.getUserId() != null) {
          behaviorMapper.insertVisitUser(event.getShopId(), event.getUserId(), bucket);
        }
        return null;
      });
}
```

以下为 [BehaviorMapper.xml](../src/main/resources/mapper/BehaviorMapper.xml) 的相关源码片段。为便于阅读只展示当前部分，依赖与 import 请查看原文件；无需把片段另建类复制运行。

```sql
INSERT IGNORE INTO tb_behavior_event(event_id,shop_id,user_id,action,occurred_at_ms,bucket_ms)
VALUES(#{eventId},#{shopId},#{userId},#{action},#{occurredAtMs},#{bucketMs})
```

以下为 [BehaviorMapper.xml](../src/main/resources/mapper/BehaviorMapper.xml) 的相关源码片段。为便于阅读只展示当前部分，依赖与 import 请查看原文件；无需把片段另建类复制运行。

```sql
INSERT INTO tb_shop_heat_bucket(shop_id,bucket_ms,visits,orders,payments)
VALUES(#{shopId},#{bucketMs},#{visits},#{orders},#{payments})
ON DUPLICATE KEY UPDATE visits=visits+VALUES(visits),orders=orders+VALUES(orders),payments=payments+VALUES(payments)
```


事件先 INSERT IGNORE，返回 0 时核对已有内容，一致才认重复。新事件才累加桶。若桶更新失败，事务回滚包括事件插入，后续重试仍能统计。若先单独提交 eventId 再累计桶，失败后 eventId 反而会挡住补统计。

ON DUPLICATE KEY UPDATE 在 `(shopId,bucketMs)` 的同一分钟行上加计数，而不是每次覆盖成 1；不同 action 传不同增量。

<a id="section-11"></a>

## 11. 分钟窗口与 UV 的数字例子

| 时间 | 用户 | 动作 | PV 增量 | 单窗口 UV 身份 |
| --- | --- | --- | --- | --- |
| 12:00 | A | VISIT | 1 | A |
| 12:00 | A | VISIT | 1 | 仍 A |
| 12:01 | A | VISIT | 1 | 仍 A |
| 12:01 | B | VISIT | 1 | A、B |
| 12:02 | 匿名 | VISIT | 1 | 仍 A、B |

同窗口 PV=5、loggedUV=2。假设还有订单 1、模拟支付 1，热度是 `5 + 2×2 + 1×5 + 1×10 = 24`。这些是教学演算，不是当前在线商家的实测值。

分分钟计算 UV 后相加会把 A 算两次，所以当前查询在整个窗口 COUNT DISTINCT user_id。分钟桶可以直接 SUM PV/orders/payments，但 UV 要用不同聚合口径。

以下为 [BehaviorAggregationServiceImpl.java](../src/main/java/com/hmdp/service/impl/BehaviorAggregationServiceImpl.java) 的相关源码片段。为便于阅读只展示当前部分，依赖与 import 请查看原文件；无需把片段另建类复制运行。

```java
public List<Map<String, Object>> ranking(int minutes, int limit) {
  if ((minutes != 5 && minutes != 60) || limit < 1 || limit > 100) {
    throw new IllegalArgumentException("窗口仅支持 5 或 60 分钟，条数 1～100");
  }
  long now = System.currentTimeMillis() / 60000 * 60000;
  long from = now - (minutes - 1L) * 60000;
  // COUNT DISTINCT over the entire window avoids adding per-minute UVs and over-counting
  // repeat visitors.
  return behaviorMapper.selectRanking(from, now, from, now, limit);
}
```

以下为 [BehaviorMapper.xml](../src/main/resources/mapper/BehaviorMapper.xml) 的相关源码片段。为便于阅读只展示当前部分，依赖与 import 请查看原文件；无需把片段另建类复制运行。

```sql
SELECT b.shop_id,s.name,SUM(b.visits) AS pv,COALESCE(u.uv,0) AS logged_uv,SUM(b.orders) AS orders,SUM(b.payments) AS mock_payments,SUM(b.visits)+COALESCE(u.uv,0)*2+SUM(b.orders)*5+SUM(b.payments)*10 AS heat
FROM tb_shop_heat_bucket b
JOIN tb_shop s ON s.id=b.shop_id
LEFT JOIN (SELECT shop_id,COUNT(DISTINCT user_id) AS uv
FROM tb_shop_visit_user
WHERE bucket_ms BETWEEN #{uvFromMs} AND #{uvToMs}
GROUP BY shop_id) u ON u.shop_id=b.shop_id
WHERE b.bucket_ms BETWEEN #{bucketFromMs} AND #{bucketToMs}
GROUP BY b.shop_id,s.name,u.uv
ORDER BY heat DESC,b.shop_id LIMIT #{limit}
```


5 分钟窗口用当前分钟及前 4 分钟，60 分钟用当前及前 59 分钟，边界是分钟开始点。发生在未来错误时间的事件可能不落入当前窗口；时间校验与时钟同步应一起理解。

<a id="section-11-01"></a>

### 快照与事实的区别再看一次

Redis 热榜是 15 秒短快照，后台约 5 秒刷新；MySQL 分钟桶是已处理事实的汇总。快照没了可以从桶重建，浏览事件根本没发送成功则无法重建。恢复副本与补回丢失原始事件是两件事。

<a id="section-12"></a>

## 12. 如何验证重放没有重复计数

选择隔离事件固定 eventId、shopId、action、userId、occurredAt，处理一次后记桶；再次处理相同内容应不再加；同 eventId 改内容应冲突隔离，而不是覆盖；模拟 offset 提交失败后重放也应保持累计值。

访问事件一旦换 UUID，就是另一条事件，会按新访问累计。为了验证幂等却每次随机生成 eventId，会测试错对象。页面的三次浏览故意是三个不同事件，应得到三条回执。

<a id="section-13"></a>

## 13. 分析 Outbox 的发送确认为什么还不够

分析 Outbox 使用租约领任务、发送后记录检查点，并对已发送但没有去重事实的事件进行后续检查与重驱动。重驱动仍用原 eventId，消费者通过事实主键去重。

管理员可查看 `/ops/analytics/outbox` 和 `/ops/analytics/failures`，对指定分析事件重试。重试之前先确认 Topic、消费组、Kafka lag 与消费者运行指标，不直接删事实表或重置全组 offset。

<a id="section-14"></a>

## 14. 体验页怎样提供可信证据

点击“产生 3 次浏览”时，服务通过正常 Kafka 发送路径提交 3 个真实 UUID 事件，页面随后用这 3 个 ID 查询 MySQL 持久事实。显示 3/3 回执，说明这三条确实到达了持久化消费环节。

此观测接口不直接插入统计表制造成功。页面不会盲目重试 POST；若超过等待时间仍未齐，保留未确认状态并允许刷新回执。不要根据热榜 PV 增加 3 就反推这三条都成功，因为其他访客也可能贡献数据；匹配 eventId 的回执更有说服力。

匿名点击后 logged UV 不增长正常。使用登录账号反复点击，窗口 UV 最多增加这个用户的一个身份。短时间两次热榜刷新可能仍是同一快照，等下一轮再观察。

<a id="section-15"></a>

## 15. 扩展方向与边界

后续可增加匿名访客标识、HLL 大规模近似 UV、反作弊、分区容量规划、数据保留清理和独立分析服务。它们需要先确定统计口径与误差预算；不能只加一个 Redis PFADD 就宣称完整用户分析平台。

当前本地 Kafka 单节点单副本没有生产容灾保障，72 小时保留不是无限历史。换消费组从 earliest 读取还留存的日志，会触发重放；事实主键能避免已有事件二次累计，但被清理的历史无法靠 offset 找回。

<a id="section-16"></a>

## 16. 从原始事件手算 PV、UV、窗口与热度

<a id="section-16-01"></a>

### 先准备五条事件，再计算答案

假设同一家店在同一统计窗口内收到：用户甲两条不同 eventId 的 VISIT、用户乙一条 VISIT、匿名访客一条 VISIT、用户甲的第一条 VISIT 重放一次。四条独立事件应使 PV=4，重放不再加一；logged_uv=2，因为甲去重、乙计入，匿名没有登录身份。

再增加一条 ORDER 和一条 PAID，当前热度公式是 `PV + logged_uv × 2 + orders × 5 + mock_payments × 10`，演算结果为 `4 + 2×2 + 1×5 + 1×10 = 23`。这是 [BehaviorMapper](../src/main/resources/mapper/BehaviorMapper.xml) 中的确定性规则，不是训练得到的推荐模型。

匿名 PV 能增加而登录 UV 不增加，是统计定义，不是丢消息。支付字段当前表示模拟支付行为，不能将它解释成真实资金成交量。演示时把指标名称连同口径一起讲，避免只有“热度 23”却说不清来源。

<a id="section-16-02"></a>

### eventId 去重与用户去重分别在回答什么

eventId 去重回答“是不是同一条采集事件再次送达”，用户去重回答“这个统计窗口里是不是同一位登录访客”。用户甲真的访问两次，有两个不同 eventId，PV 应增加两次；同一个 eventId 投递两次，PV 只增加一次。这两种重复不能用同一个规则处理。

事实表以 event_id 唯一；若重复插入返回 0，还要核对原内容相同。同 ID 改为另一家店或另一动作属于身份冲突，应拒绝，而不是把数据错误掩盖成正常重复。

登录 UV 的持久记录包含店铺、用户和分钟桶。最终窗口用 `COUNT(DISTINCT user_id)` 重新跨桶去重，不能直接把每分钟 UV 相加。用户甲在 10:01 和 10:02 都来过，两桶分别有甲，五分钟窗口仍只能算一个甲。

<a id="section-16-03"></a>

### 五分钟窗口并非逐毫秒滑动的最近 300 秒

当前用 `occurredAt / 60000 * 60000` 对齐分钟。排名时把当前时间对齐到分钟起点，读取当前桶以及前四个桶，共五个桶；60 分钟则读取当前及前 59 个桶。

教学例子：当前 10:04:30，五分钟窗口对应 10:00、10:01、10:02、10:03、10:04。它覆盖了这些整分钟桶，不等价于精确从 09:59:30 开始的 300 秒窗口。边界附近比较结果时，先看桶的起点。

事件按发生时间归桶，不是按消费时间。10:01 发生、10:05 才消费的事件仍进入 10:01 桶；如果查询窗口已越过该桶，事实存在也未必显示在当前排名。消息消费延迟与榜单更新时间是两个不同原因。

<a id="section-16-04"></a>

### 数据库提交与 offset 确认的先后推演

设消费者读到分区 0 的 offset=100。先提交事件事实与桶累加，再确认消费进度。如果提交后确认前退出，重启可能再处理 100；事实去重使第二次不再累加。

若反过来先确认 offset，再写数据库，退出后 Broker 可能认为已经处理，事实却缺失。当前代码把业务落地放在确认之前，换取可能重放，再由幂等承受。Producer 幂等减少生产协议内某类重复，不替代这里的消费数据库去重。

Kafka offset 按分区维护。分区 0 的 offset=100 和分区 1 的 offset=100 不是同一条消息，也不能以这两个数字比较全局先后。当前按店铺构造消息 Key，有助于同店事件落在同一分区；跨店没有由此获得全局顺序。

<a id="section-16-05"></a>

### 浏览事件和订单事件为什么不用同一种投递保证

店铺浏览是尽力异步采集，正常页面访问不等待 Kafka 确认。返回浏览 eventId 只是建立追踪身份，采集压力或发送故障时可以未落地。因此提交 3 条体验浏览，不保证永远看到 3 条 PERSISTED。

订单创建与支付关联分析由持久来源事件生成分析 Outbox，再等待 Kafka 发送确认。若已 SENT 但超过观察窗口仍没有事实记录，会沿原 eventId 重驱动。订单事实不应因短暂分析不可用而失去后续投递依据。

当前秒杀履约尚未统一进入普通订单的 ORDER_CREATED 生命周期，所以不要假定所有秒杀都已同口径贡献 orders。发现这个缺口可以作为后续真实改进目标，文档不能先把计划写成已完成。

<a id="section-16-06"></a>

### 热榜快照为什么落库后还会慢一点变化

事件事实与分钟桶落库后，热榜还有 Redis 快照刷新。当前定时刷新间隔为 fixedDelay 5 秒，快照 TTL 15 秒；处理耗时和调度也会影响下一次刷新，不能承诺每条事件精确五秒内显示。

5 与 60 分钟窗口分别保存最多 100 条快照，查询时按 limit 截取。快照缺失或读取不可用时，回到持久分钟桶计算。这个降级保护榜单可读，却可能增加数据库查询量；不能把“可以降级”理解成数据库不会受影响。

<a id="section-16-07"></a>

### 一次可信的页面实验该留下什么

保存提交响应的原 eventIds，查询对应回执直到出现 PERSISTED 或达到你记录的等待上限。随后观察同店 PV 与 UV，并记录窗口、时间和是否登录。排名没立即变化时，依次检查事实是否落地、桶是否在窗口、快照是否刷新，而不是立刻重复提交大量新事件。

若要证明重复消费不重复计数，阅读或运行隔离去重测试，重复的是同一个 eventId；再点击体验按钮会生成新 UUID，新增 PV 是正确行为，不能据此判断幂等失效。
