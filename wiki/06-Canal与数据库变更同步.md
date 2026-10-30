# 06 Canal 与数据库变更同步

[返回目录](README.md) · [上一章](05-五层缓存与一致性.md) · [下一章：优惠券订单](07-优惠券与订单生命周期.md)

**本章学习目标**：本章先讲直接改数据库为什么绕过应用失效，再解释 binlog、订阅、投影、检查点与 ACK。读完应能说明一条券归属变更会影响哪些缓存，以及失败批次为什么可以重放。

**本章导航**

- [1. 为什么应用删缓存还不够](#section-01)
- [2. MySQL 为什么要 ROW 与 FULL](#section-02)
- [3. CDC 读到的行为什么不直接写回缓存](#section-03)
- [4. 批次处理与 ACK](#section-04)
- [5. 检查点要分清两种用途](#section-05)
- [6. DDL 与批量失效](#section-06)
- [7. 从一条 UPDATE 看懂 binlog 与 Canal](#section-07)
  - [业务库和缓存库为什么不自动同步](#section-07-01)
  - [一条券迁店事件的演算](#section-07-02)
  - [当前不同表的处理方式](#section-07-03)
  - [阅读投影器时的顺序](#section-07-04)
- [8. 从 getWithoutAck 推导批次可靠性](#section-08)
- [9. 自己做一次数据库直改验证](#section-09)
  - [断线后你应该看到什么](#section-09-01)
- [10. 如何验证数据库直改也会影响页面](#section-10)
- [11. Canal 故障时如何理解现象](#section-11)
- [12. 本章面试说法](#section-12)
- [13. 用一笔修改建立 CDC 的完整认识](#section-13)
  - [CDC 看的是已经提交的变化，不是浏览器动作](#section-13-01)
  - [一张券迁店，实际影响不止一个 Key](#section-13-02)
  - [为什么库存变化不能顺手同步到秒杀 Redis](#section-13-03)
  - [批次内第三条失败，前两条是不是白做了](#section-13-04)
  - [一次能分辨传播来源的实验](#section-13-05)
  - [断线恢复何时需要重建基线](#section-13-06)

<a id="section-01"></a>

## 1. 为什么应用删缓存还不够

管理员在数据库工具里直接把店铺名称修改了，Java 没有收到修改请求，就不会新增应用失效任务。浏览器继续读缓存，仍显示旧名称。

MySQL 的 binlog 记录数据库变更。Canal 模拟订阅这份日志，Java 从 Canal 读取变更批次，识别受影响的店铺或券，并使缓存失效。这就是当前 CDC 链路。

```text
MySQL 提交变更 → binlog → Canal → Java CanalCacheConsumer
→ CacheChangeProjector → 删除 Redis 数据、推进版本、通知本地缓存
```

核心源码：[CanalCacheConsumer](../src/main/java/com/hmdp/cache/CanalCacheConsumer.java)、[CacheChangeProjector](../src/main/java/com/hmdp/cache/CacheChangeProjector.java)、[CacheCheckpointServiceImpl](../src/main/java/com/hmdp/service/impl/CacheCheckpointServiceImpl.java)。

<a id="section-02"></a>

## 2. MySQL 为什么要 ROW 与 FULL

当前 Compose MySQL 开启 binlog，`binlog_format=ROW`、`binlog_row_image=FULL`，并配置 server-id 和专用 Canal 订阅账号。ROW 记录行变化，FULL 为变更提供较完整的前后列信息，便于识别主键和关联店铺变化。

例如一张券从店铺 A 调整到店铺 B，需要同时失效 A、B 的券列表。只看更新后的 shop_id，就可能漏掉旧店铺 A。投影器会考虑前后关联，对单条券变更中的旧、新店铺 ID 去重后失效。当前批次循环仍逐条调用 apply，尚未把整批所有重复目标统一归并。

当前订阅重点是 `tb_shop`、`tb_voucher`、`tb_seckill_voucher`。不是订阅全库的所有表，也不是用 Canal 代替所有业务消息。

<a id="section-03"></a>

## 3. CDC 读到的行为什么不直接写回缓存

假设 Canal 正在补处理旧日志，而数据库又发生了更新。如果把旧 binlog 行直接当当前对象写入缓存，就可能把过时数据复活。

项目以失效为主；店铺和券变更后还会主动读取当前数据库重建相应元数据。当前店铺 CDC 重建基础 TTL 是 20 分钟，而普通详情回源常量是 30 分钟；两者并不统一。涉及重建时以当前数据库状态为依据。Canal 负责指出“哪里变了”，数据库提供“现在是什么”。这与当前缓存版本栅栏一起减少过时回填。

秒杀交易 Redis 状态有预占和在途请求，不能把数据库中的剩余库存简单覆盖到 Redis。CDC 的缓存失效和秒杀恢复是不同操作，库存恢复必须考虑预约事实与在途消息。

<a id="section-04"></a>

## 4. 批次处理与 ACK

Java 从 Canal 获取一个批次后：解析变更、处理失效、记录检查点，再确认批次。失败时不当作成功 ACK，而是回滚订阅批次并在重连后继续。

“至少一次”意味着某批可能被重复投影。删除缓存与推进版本必须允许重复；重复失效主要带来额外加载，不应让业务数据重复扣减。

当前用 Redis 租约协调活动 CDC 消费者，降低多个 Java 实例同时投影的竞争。租约与网络异常仍需要幂等投影兜底，不能宣传只处理一次。

有些批次只有事务边界，没有监听目标行；这类批次仍要正确确认，避免检查点表自己产生的事务持续干扰消费进度。

<a id="section-05"></a>

## 5. 检查点要分清两种用途

`tb_cache_cdc_checkpoint` 保存 destination 与消费进度信息，便于审计“最后处理到哪里”和定位延迟。它不是 Java 随便取出一个 batchId 就能让 Canal 从任意位置恢复的完整机制。

Canal 自身的 `canal-meta` 卷保存订阅元数据，MySQL 保留的 binlog 决定历史能追多久。当前开发配置 binlog 保留约 7 天。如果断线太久，所需日志已被清理，不能指望自动重放恢复；需要重新建立订阅基线并重建相关缓存。

<a id="section-06"></a>

## 6. DDL 与批量失效

表结构变更等 DDL 可能使已有缓存结构不再适用。项目对相应命名空间使用 Redis SCAN 分批失效，而不是在正常业务里对全 Redis 执行阻塞式 KEYS。

SCAN 不是全库一致性快照，也不是“零成本”。大规模结构变化仍需要考虑并发写入和扫描时长，不能把当前教学规模的实现直接当成任意大数据量的无风险操作。

<a id="section-07"></a>

## 7. 从一条 UPDATE 看懂 binlog 与 Canal

<a id="section-07-01"></a>

### 业务库和缓存库为什么不自动同步

执行 `UPDATE tb_shop SET name=... WHERE id=...` 时，MySQL 改的是自己的行。Redis 没有直接订阅 MySQL 表，Java 也不一定经过这次写操作。binlog 为我们提供已经发生的变更记录，Canal 读取它再转给应用。

数据库的 SQL 文本、binlog 行事件、Canal Entry 和业务缓存 Key 是四个层次。Canal 读到的是变更记录，Java 还要解释表名、主键和前后关联，才能决定删哪个 Key。

<a id="section-07-02"></a>

### 一条券迁店事件的演算

假设券 7 从店铺 1 移到店铺 2：

| 数据 | 修改前 | 修改后 | 要处理的缓存 |
| --- | --- | --- | --- |
| tb_voucher.id | 7 | 7 | cache:voucher-meta:7 |
| tb_voucher.shop_id | 1 | 2 | cache:voucher-list:1 与 :2 |

只删 `cache:voucher-meta:7` 不够，两个店铺的券目录都可能旧。使用 FULL row image 有助于取得旧关联，但代码仍要处理缺少主键/异常内容，不能盲目猜 ID。

<a id="section-07-03"></a>

### 当前不同表的处理方式

| 表或事件 | 实际动作 | 为什么 |
| --- | --- | --- |
| tb_shop 行变化 | 失效详情；非删除时从当前库主动重建 | 防止继续用旧展示数据 |
| tb_voucher 行变化 | 失效券元数据与前后店铺目录；非删除时重建元数据 | 券属性与聚合目录关联 |
| tb_seckill_voucher 变化 | 当前分支不把库存投影回普通元数据缓存 | 活动时间/库存本来实时读取；Redis 预占不是展示库存 |
| 相关 DDL | 对相关缓存命名空间失效 | 旧序列化结构可能不适配 |
| 未监听表/事务边界 | 无业务投影，按批确认规则处理 | 避免无关事务阻塞进度 |

因此“监听某张表”不代表每一列变化都会直接更新执行库存。交易恢复有自己的流程。

<a id="section-07-04"></a>

### 阅读投影器时的顺序

先看 relevant：是否 ROWDATA、schema 是否匹配、是否在三张表的范围。再看 apply：DDL 处理，行循环，before/after 解析，表类型分支，失效与必要的当前库重读。

以下为 [CacheChangeProjector.java](../src/main/java/com/hmdp/cache/CacheChangeProjector.java) 的相关源码片段。为便于阅读只展示当前部分，依赖与 import 请查看原文件；无需把片段另建类复制运行。

```java
public boolean isRelevantEntry(CanalEntry.Entry entry) {
  return entry.getEntryType() == CanalEntry.EntryType.ROWDATA
      && schema.equals(entry.getHeader().getSchemaName())
      && WATCHED_TABLES.contains(entry.getHeader().getTableName());
}
```

以下为 [CacheChangeProjector.java](../src/main/java/com/hmdp/cache/CacheChangeProjector.java) 的相关源码片段。为便于阅读只展示当前部分，依赖与 import 请查看原文件；无需把片段另建类复制运行。

```java
/**
 * 仅处理指定数据库和表的变更，失效相关元数据缓存并按需回源。
 *
 * <p>活动执行库存和购买资格由秒杀流程维护，CDC 不用数据库快照覆盖 Redis 执行状态。
 */
public void projectEntryChanges(CanalEntry.Entry entry) throws InvalidProtocolBufferException {
  log.debug(
      "CDC entry: {} {}.{} type={}",
      entry.getEntryType(),
      entry.getHeader().getSchemaName(),
      entry.getHeader().getTableName(),
      entry.getHeader().getEventType());
  if (!isRelevantEntry(entry)) {
    return;
  }
  String table = entry.getHeader().getTableName();
  CanalEntry.RowChange change = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
  if (change.getIsDdl()) {
    invalidateTableMetadata(table);
    metrics.counter("hmdp.cdc.ddl", "table", table).increment();
    return;
  }
  log.debug("CDC row count={} event={}", change.getRowDatasCount(), change.getEventType());
  for (CanalEntry.RowData row : change.getRowDatasList()) {
    projectRow(table, change.getEventType(), row);
    metrics.counter("hmdp.cdc.rows", "table", table).increment();
  }
}

private void invalidateTableMetadata(String table) {
  if ("tb_shop".equals(table)) {
    cache.invalidatePrefix("cache:shop:");
  }
  if ("tb_voucher".equals(table)) {
    cache.invalidatePrefix("cache:voucher-meta:");
    cache.invalidatePrefix("cache:voucher-list:");
  }
}

private void projectRow(String table, CanalEntry.EventType eventType, CanalEntry.RowData row) {
  Map<String, String> before = mapColumnValues(row.getBeforeColumnsList());
  Map<String, String> after = mapColumnValues(row.getAfterColumnsList());
  Map<String, String> identity = after.isEmpty() ? before : after;
  if ("tb_shop".equals(table)) {
    projectShopChange(parseRowId(identity, "id"), eventType);
  } else if ("tb_voucher".equals(table)) {
    projectVoucherChange(parseRowId(identity, "id"), eventType, before, after);
  }
  // 活动库存与窗口实时查库；CDC 不覆盖 Redis 的执行库存或购买资格。
}

private void projectShopChange(long shopId, CanalEntry.EventType eventType) {
  cache.invalidate("cache:shop:" + shopId);
  if (eventType != CanalEntry.EventType.DELETE) {
    cache.queryWithMutex(
        "cache:shop:",
        shopId,
        Shop.class,
        id -> shopServiceProvider.getObject().getById(id),
        CDC_SHOP_TTL_MINUTES,
        TimeUnit.MINUTES);
  }
}

private void projectVoucherChange(
    long voucherId,
    CanalEntry.EventType eventType,
    Map<String, String> before,
    Map<String, String> after) {
  cache.invalidate("cache:voucher-meta:" + voucherId);
  // 券换店后旧、新店铺都需要失效；这里只对当前行的关联店铺去重。
  Set<String> shopIds = new HashSet<>();
  shopIds.add(before.get("shop_id"));
  shopIds.add(after.get("shop_id"));
  shopIds.remove(null);
  for (String shopId : shopIds) {
    cache.invalidate("cache:voucher-list:" + Long.parseLong(shopId));
  }
  if (eventType != CanalEntry.EventType.DELETE) {
    cache.queryWithMutex(
        "cache:voucher-meta:",
        voucherId,
        Voucher.class,
        id -> voucherServiceProvider.getObject().getById(id),
        VOUCHER_METADATA_TTL_MINUTES,
        TimeUnit.MINUTES);
  }
}
```


`identity=after.isEmpty()?before:after` 用于确定变更主键；删除时 after 可能没有行，要从 before 找身份。对券目录使用 Set 去重前后店铺；这个 Set 仅属于当前行处理，并非整批统一去重器。

`ObjectProvider` 延迟取得 Service，减少初始化依赖循环；它不表示开启一个新微服务，也不改变方法最终仍查询当前 MySQL 的事实。

<a id="section-08"></a>

## 8. 从 getWithoutAck 推导批次可靠性

getWithoutAck 表示先取批次，成功处理后才确认。假设一个批次有 10 条，前 8 条删缓存成功、第 9 条失败；如果已经 ACK 整批，剩余失败就不容易重放。当前失败会 rollback 批次，前 8 条可能再执行一次，所以投影要允许重复。

以下为 [CanalCacheConsumer.java](../src/main/java/com/hmdp/cache/CanalCacheConsumer.java) 的相关源码片段。为便于阅读只展示当前部分，依赖与 import 请查看原文件；无需把片段另建类复制运行。

```java
void processBatch(CanalConnector connector, Message batch) {
  try {
    boolean relevant = false;
    for (CanalEntry.Entry entry : batch.getEntries()) {
      if (projector.isRelevantEntry(entry)) {
        relevant = true;
      }
      projector.projectEntryChanges(entry);
    }
    // Checkpoint writes themselves produce transaction boundaries in binlog.
    // Ack batches containing only excluded tables without writing another checkpoint.
    if (!relevant) {
      connector.ack(batch.getId());
      metrics.counter("hmdp.cdc.batches", "result", "ignored").increment();
      return;
    }
    CanalEntry.Header tail = batch.getEntries().get(batch.getEntries().size() - 1).getHeader();
    cacheCheckpointService.saveCheckpoint(
        destination,
        batch.getId(),
        tail.getLogfileName(),
        tail.getLogfileOffset(),
        tail.getExecuteTime());
    connector.ack(batch.getId());
    metrics.counter("hmdp.cdc.batches", "result", "success").increment();
    if (tail.getExecuteTime() > 0) {
      metrics
          .timer("hmdp.cdc.event.delay")
          .record(
              Math.max(0, System.currentTimeMillis() - tail.getExecuteTime()),
              TimeUnit.MILLISECONDS);
    }
  } catch (Exception failure) {
    metrics.counter("hmdp.cdc.batches", "result", "retry").increment();
    connector.rollback(batch.getId());
    throw new IllegalStateException("缓存 CDC 批次失败，保留位点重放", failure);
  }
}
```


分段解释：

1. 遍历 Entry，每条都调用投影器，同时记录批次是否包含目标表。
2. 全是无关内容时直接确认，避免检查点写入产生新事务边界后无限写检查点。
3. 对有实际目标变更的批次，保存尾部文件名、偏移与执行时间等审计信息。
4. 保存后调用 ack；如果 ack 抛异常，整体走 rollback/retry。
5. 记录消费延迟，负时间差按 0 处理，避免时钟差造成负指标。

保存检查点与 Canal ack 不是一个跨组件原子事务。保存了审计信息不等于 Canal 已确认；重复处理仍可能发生。缓存失效能重复，但这段代码不适合直接复制去做非幂等扣款。

<a id="section-09"></a>

## 9. 自己做一次数据库直改验证

先在自己的演示环境选一条店铺，记录 id 和原始名称，不使用别人正在体验的店铺。可以进入 Compose MySQL 容器后连接库；以下是数据库客户端中的教学操作，实际 ID/名称要替换。

```sql
SELECT id,name FROM tb_shop WHERE id = 你的演示店铺ID;
UPDATE tb_shop SET name = '文档CDC测试名称' WHERE id = 你的演示店铺ID;
SELECT id,name FROM tb_shop WHERE id = 你的演示店铺ID;
```

确认数据库已改，再看 Java CDC 日志、检查点与页面。最后 UPDATE 恢复事先记录的原名，并再次验证传播。不要省略原值记录，也不要把“你的演示店铺ID”原样当合法 SQL。

最有说服力的记录包括：修改前页面、数据库提交证据、CDC 处理时间/批次、修改后页面。如果只有前后截图，不能排除缓存自然过期造成更新。

<a id="section-09-01"></a>

### 断线后你应该看到什么

在专用隔离环境暂停 Canal，数据库直改可能暂时不反映；恢复后若 binlog 仍保留，消费追上并失效。如果 Java 写接口的 Outbox 仍正常，它的更新可能照常反映。两条更新来源必须分别验收。

不建议在共享演示环境随意停组件或删元数据卷。要演示故障，把影响局限在隔离项目，并保存操作前后状态。

<a id="section-10"></a>

## 10. 如何验证数据库直改也会影响页面

建议在隔离测试库执行受控验证，已有 [CacheCdcIntegrationTest](../src/test/java/com/hmdp/cache/CacheCdcIntegrationTest.java) 可供阅读，具体文件存在情况以测试目录为准。若手工在本地演示库操作，先选择自己的演示店铺并记录原值：

1. 打开店铺，确认缓存已经产生。
2. 使用数据库工具修改该店铺的测试名称，不走 Java 写接口。
3. 观察 Canal/Java 日志、CDC 检查点和缓存失效指标。
4. 刷新页面，等待消费和短缓存过期后确认新名称。
5. 恢复原值，再确认恢复也可以传播。

第 4 步出现变化仅能说明数据最终更新了；要证明走 Canal，需要同时查看变更消费证据，避免恰好缓存 TTL 到期造成误判。页面普通访客不能直接执行数据库修改与 Canal 断线测试，体验页会把历史隔离证据与当前读路径分别展示。

<a id="section-11"></a>

## 11. Canal 故障时如何理解现象

Java 正常、店铺页面能打开，并不表示 CDC 正常。Canal 断线后，应用内更新可能仍通过 Outbox 失效，而数据库直改无法及时传播；两条路径的表现不同。

排查顺序：Canal 服务是否可用 → 连接 MySQL 的账号与权限 → binlog 设置 → destination 和过滤表 → Java 消费者租约 → 最近检查点时间 → 是否已超出 binlog 保留范围。

缓存过时首先是查询一致性问题，不能用“把所有业务 Redis 清空”来修复，特别是不能误删登录、签到和秒杀状态。准确识别缓存命名空间，再按受影响范围处理。

<a id="section-12"></a>

## 12. 本章面试说法

“应用事务内写入缓存失效 Outbox 处理正常更新，Canal 监听数据库直接变更作为补充。CDC 按批逐条解析，券变更会对单行的旧、新关联店铺去重失效，成功后记录检查点再 ACK，失败重试。缓存回填通过版本和锁 owner 防止旧加载污染。但通知与 CDC 都存在延迟，系统是有边界的最终一致性。”

追问断线超过日志保留时间时，要承认需要重建基线，而不是说“有 checkpoint 就永远能恢复”。

<a id="section-13"></a>

## 13. 用一笔修改建立 CDC 的完整认识

<a id="section-13-01"></a>

### CDC 看的是已经提交的变化，不是浏览器动作

用户通过 Java 修改店铺，与管理员在数据库工具里直接 UPDATE 店铺，最终都可能产生 MySQL binlog。Canal 读取数据库变更日志，再由 Java 的 CDC 消费者解析。浏览器只是其中一种写入来源，不是 Canal 的上游协议。

因此 CDC 不是“每隔几秒查询店铺表”。它关注已提交事务产生的变更记录，并按日志位置继续读取。MySQL 回滚的那次业务修改不会变成同样的已提交行变化。阅读这条链路时，先确认数据库提交，再讨论 Canal 接收到哪个批次。

ROW 表示日志记录行变化；FULL 提供完整行映像，有利于拿到关联字段。例如券从店铺 101 移到 202，需要知道旧 shop_id 和新 shop_id，分别让两个店铺的券列表失效。只清新店铺，会让旧店铺继续展示已经迁走的券。

<a id="section-13-02"></a>

### 一张券迁店，实际影响不止一个 Key

假设教学券 ID=700，before.shop_id=101、after.shop_id=202。投影器先失效 `cache:voucher-meta:700`，再对该行的旧、新店铺去重，失效 `cache:voucher-list:101` 与 `cache:voucher-list:202`。若两边仍是同一家店，去重后只处理一次。

处理 INSERT 时没有旧行、处理 DELETE 时没有新行，因此身份提取必须按事件类型和实际列内容理解。删除后不能再按“回源当前券并写缓存”的正常新增流程处理；库中已经不存在这条记录。

当前店铺 CDC 回源基础 TTL 是 20 分钟，普通详情路径基础 TTL 是 30 分钟；券元数据基础 TTL 是 10 分钟，实际写入可能有抖动。它们是当前实现差异，不要在演示时把所有详情 TTL 都说成固定 30 分钟。

<a id="section-13-03"></a>

### 为什么库存变化不能顺手同步到秒杀 Redis

券元数据是查询副本，秒杀执行库存却已经承担准入职责。假设 MySQL 库存仍为 5，Redis 已受理 2 人但后台履约还未完成，Redis 剩余是 3。此时用数据库的 5 覆盖 Redis，会把已受理的两份库存再次开放。

因此投影器不会把 `tb_seckill_voucher` 的数据库库存直接复制到 Redis 执行库存。执行库存恢复需要检查未完成工作、购买资格和活动基线，属于秒杀发布/恢复流程。表名里有“库存”不意味着任何 CDC UPDATE 都能驱动运行中库存重置。

这是阅读“监听数据库变更”的重要边界：CDC 可以传递变化，但不能替代业务语义。复制完整行并不自动保证正确，尤其是对已经接收请求的执行状态。

<a id="section-13-04"></a>

### 批次内第三条失败，前两条是不是白做了

假设批次 B 有三条相关变化，前两条失效成功，第三条由于依赖故障抛异常。当前消费者不会 ACK B，而是 rollback 保留重放责任。之后三条都可能重新处理。

前两条不是数据库回滚到未失效状态，而是下次重复失效。失效通常能重复，重复增加缓存版本只会继续淘汰旧副本；这也是该处理方式能够承受整批重放的基础。若把相同循环改成直接扣款或无条件库存加一，整批重放就可能产生重复副作用。

处理相关批次后会记录检查点，再向 Canal ACK。数据库记录成功但 ACK 丢失，仍可能再次收到批次；不要把检查点记录当成跨数据库与 Canal 的原子确认。它提供观察依据，Canal 自身的消费位点负责续读。

<a id="section-13-05"></a>

### 一次能分辨传播来源的实验

先读演示店铺，让查询副本存在，再记录名称与时间。通过数据库工具修改这一条演示记录，不经过 Java 写接口；确认提交成功。随后观察 CDC 批次日志、检查点时间和页面新值，最后恢复原值。

如果通过网页修改，应用提交后的失效与 Outbox 也可能起作用，只凭页面变化无法证明 Canal 参与。因此本实验限定写入来源。反过来，也不要因为数据库直改暂时没传播就认为 Java 写接口一定坏了，两条链路应分别验收。

| 现象 | 应继续检查 | 暂时不能下的结论 |
| --- | --- | --- |
| 数据库仍是旧值 | UPDATE 是否提交、连接了哪个库 | Canal 丢消息 |
| 库已变，检查点没有推进 | 过滤规则、连接、消费者处理异常 | 页面实现有问题 |
| 检查点推进，但页面暂时旧值 | 上层短副本、采样时刻、店铺 ID | 数据库更新失败 |
| 页面更新但无 CDC 证据 | 是否走了应用失效、是否自然过期 | CDC 故障恢复已验证 |

<a id="section-13-06"></a>

### 断线恢复何时需要重建基线

短暂断线后，若消费位点和对应 binlog 仍存在，消费者可以继续追赶。断线超过日志保留范围，就不能凭一个旧 offset 读取已删除的日志。当前开发 Compose 的 MySQL binlog 保留设置为 604800 秒，也就是 7 天；这不是无限历史。

需要重建基线时，应明确目标缓存范围和数据库当前事实，同时确保后续变化不被漏掉。本文没有实现完整在线快照加增量切换平台，不能把“保存检查点”描述成已经具备生产级全量重建与灾备能力。初学者至少要能说明断线追赶与日志缺口恢复是两种问题。
