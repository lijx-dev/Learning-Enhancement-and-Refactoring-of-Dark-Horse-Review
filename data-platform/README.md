# 离线数据开发模块

已实现轻量主链：只读MySQL一致性抽取 → ODS冻结批次 → Spark SQL DIM/DWD/DWS/ADS → 质量门禁 → 版本发布。已用手算样本与真实MySQL读取验证，完成本机1万/10万合成订单的SQL性能实验，以及Airflow参数化运行、自动重试与迟到补数验收；没有分布式部署或生产吞吐结论。

- [数据契约](contracts/README.md)、[列白名单](contracts/source-contract.json)：粒度、金额、时区与未知历史。
- [可手算样本](fixtures/hand-calculable/README.md)：14笔合成订单、3个情景快照与独立人工答案。
- [分层SQL](sql/models.sql)：3个DIM、3个DWD、1个DWS、3个ADS。
- [SQL与性能实验](benchmarks/README.md)：1万/10万确定性样本、缓存对照、分区/广播/文件/热点聚合实验及复现入口。
- [本地调度](orchestration/README.md)：参数化DAG、受约束的宿主任务桥接；Airflow 2.11.0真实scheduler的按日运行、自动重试、旧版本保护及迟到补数已验收。
- [Hive兼容表管理验收](tests/acceptance_hive.py)：Spark内嵌Hive Metastore、Parquet分区外部表、分区修复、版本切换及跨进程重开；复现见下文。
- [抽取器](extract/mysql_snapshot.py)、[主链](jobs/offline_pipeline.py)、[宿主入口](tools/warehouse.py)。

## 快速复现

PowerShell在项目根目录操作；Docker需处于Linux context，使用已有 `dev/local/compose.yml` 的Spark 3.5.7。所有步骤顺序执行，单次Spark容器上限2GiB，不与完整中间件栈同时运行。

本地环境入口为 `python -X utf8 scripts/local-environment.py <action>`：`check`检查环境，`build`构建应用，`up`启动并冒烟验证，`test`运行无broker的隔离测试子集，`spark`验证JDBC/Parquet，`down`关闭本项目容器并保留数据卷。应用页面为 `http://127.0.0.1:18080`；配置中的固定密码仅供本机演示，不用于生产。纯数仓样本运行只需要Docker与Python，无需启动应用或数据库。

```powershell
python -m venv target/warehouse-venv
target/warehouse-venv/Scripts/python.exe -m pip install -r data-platform/requirements-verify.txt
python -X utf8 data-platform/tools/fixture.py check
python -m unittest discover -s data-platform/tests -v

python -X utf8 data-platform/tools/warehouse.py run --input-root target/warehouse-fixtures/hmdp_hand_v1/s3 --source-batch s3 --biz-date 2026-10-30 --run-id my-first-run
python -X utf8 data-platform/tools/warehouse.py status --biz-date 2026-10-30
target/warehouse-venv/Scripts/python.exe -X utf8 data-platform/tests/verify_models.py --run-root target/warehouse-v2 --biz-date 2026-10-30 --fixture-batch s3
```

只有抽取需要PyMySQL；Spark使用容器内Python和内置Parquet，不将PySpark安装到Windows。PyArrow仅用于独立验收，依赖文件与已验收版本固定。若网络缓慢可换可信pip镜像，不改全局Python。生成文件、虚拟环境、日志都在target下被Git忽略。

## 真实MySQL快照到报表

```powershell
docker --context desktop-linux compose -f dev/local/compose.yml up -d --wait mysql
# 可选：准备非空合成样本源库。只修改带所有者标记的 hmdp_warehouse_fixture。
python -X utf8 data-platform/tools/prepare_fixture_source.py

# 在当前终端设置 HMDP_WAREHOUSE_PASSWORD，取本地只读账号密码；不要把真实密码提交仓库。
target/warehouse-venv/Scripts/python.exe -X utf8 data-platform/extract/mysql_snapshot.py --source-batch my-source-batch --source-env synthetic_local_demo --payment-provenance mock_only --synthetic --database hmdp_warehouse_fixture
python -X utf8 data-platform/tools/warehouse.py run --input-root target/warehouse-snapshots/my-source-batch --source-batch my-source-batch --biz-date 2026-10-30 --run-id my-mysql-run --run-root target/warehouse-mysql
```

抽取器默认连 `127.0.0.1:13306`，账号warehouse_reader。读取原业务库时使用 `--database hmdp --source-env local_demo`，不加synthetic。密码只通过环境传入；源码没有记录真实凭据。抽取白名单排除用户敏感字段和非订单Outbox，账号本身的权限仍是本地配置，生产权限须独立设计。

每个source-batch必须新命名，旧批次拒绝覆盖；失败内容留在快照根目录的.staging，不会当成功批次读取。snapshot_as_of来自真实事务时间，不能指定一个过去的日期冒充历史快照。GTID/binlog不作恢复承诺，抽取期间需排除DDL。默认事务最多120秒，超时回滚并保留失败文件。

## 版本与补数

```text
target/warehouse-v2/
  catalog/2026-10-30.json              # 消费者从这里找最近成功版本
  versions/2026-10-30/<run-id>/        # 已发布，不覆盖
    ods/                             # 原始JSONL与源manifest
    dim/,dwd/,dws/,ads/               # Parquet
    quality.json,run_manifest.json    # 质量、输入/SQL哈希和产物哈希
  .staging/2026-10-30/<run-id>/        # 失败版本也保留，不可直接当正式产物
  .locks/                            # 同业务日发布锁
```

`--biz-date`决定日报和按日ADS的分区。DIM/DWD保留整个输入快照，状态报表使用实际snapshot_as_of，不被称为业务日终。所有模型带source_batch/source_env/snapshot_as_of。上游换成包含迟到事件的新批次，仍指定原业务日重跑，即修订旧日期结果；上一版本保留。

新run-id重跑相同来源结果相同，旧run-id拒绝覆盖。发布前完成质量校验；只原子替换小型catalog指针，失败不影响旧版本。status会校验manifest和产物SHA256。只适用于当前单机同文件系统；不把它当成多机事务或S3发布协议。进程被强杀后可能遗留锁，需先确认无同日期任务再人工恢复，禁止看到锁就自动删。

## 验收

```powershell
target/warehouse-venv/Scripts/python.exe -X utf8 data-platform/tests/acceptance.py --tag unique-suite-tag --run-root target/warehouse-acceptance
```

执行s1/s2/s3、手算对账、同批次重跑、发布前失败注入，保留机器报告。质量阻断包括：哈希/计数错误、非法类型/日期、重复主键、同订单多个付款事件、孤儿事件、付款时间不符、汇总金额/粒度不符。未投影事件、未知金额和缺维记录为明确警告，不静默推成0；报表要同时读取quality.json。

可通过 `--cache-policy memory-and-disk` 复用DWD/DWS中间计算，默认none；发布版本中的execution.json记录实际策略和阶段耗时。性能实验方法和验收结果见上方实验说明。调度当前是schedule=None的手动参数化触发，自动日更/抽取选批、独立Hive/HDFS/查询服务仍待建设；当前未验证退款完整事件、CDC增量删除、对象存储发布、硬故障自动恢复或生产支付。

## Hive兼容分区外部表

复用Spark 3.5.7内置Hive Metastore 2.3.9类库和Derby 10.14.2.0，不下载新镜像。实验通过enableHiveSupport创建独立Derby目录，用真正的Hive SerDe外部表DDL（STORED AS PARQUET）注册两个DWD。数据仍是本地文件系统中的已发布版本，不是HDFS；没有启动独立Metastore服务、HiveServer2或Hive执行引擎。

先准备已验证的s2、s3数仓版本（如果已有可跳过第一条，按实际目录替换参数）：

```powershell
target/warehouse-venv/Scripts/python.exe -X utf8 data-platform/tests/acceptance.py --tag hive-source-v1 --run-root target/hive-source-runs --cache-policy memory-and-disk
target/warehouse-venv/Scripts/python.exe -X utf8 data-platform/tests/acceptance_hive.py --tag my-hive-test --run-root target/hive-source-runs --previous-version target/hive-source-runs/versions/2026-10-30/hive-source-v1-s2
```

run-root/catalog的2026-10-30版本必须是合成s3，previous-version必须是合成s2的完整已发布目录；不接受失败stage。每次tag换新名，输入损坏会先被哈希校验阻断。脚本依次启动两个Spark进程：第一个注册s2、SHOW PARTITIONS、移除一条分区元数据再MSCK REPAIR恢复，随后串行将表与所有已有分区LOCATION切换到s3；第二个进程重开同一Derby目录，直接查询持久化的表和分区。

独立校验逐店创建数/去重人数/已知金额/未知金额/免费数、付款数与金额、每日关单数及分区集合，均与既有人工答案核对；PyArrow另从源Parquet计算付款关联。2026-11-05实测：s2为14订单23事件，s3为14订单25事件；10月30日模拟付款从5笔15000分修正为6笔16000分。两表各有10月29日、30日分区；EXPLAIN FORMATTED显示PartitionFilters及只选择event_date=2026-10-30的扫描位置。跨进程重开结果一致，所有源产物哈希保持不变。

输出在target/warehouse-hive-acceptance/<tag>：acceptance.json、两个Spark日志、build/reopen-report.json、执行计划、Derby目录及derby.log，全由Git忽略。本机最终验收目录为hive-20261105-final；此前失败目录保留诊断。总报告记录源码、输入与日志/计划/报告证据的SHA256。

注意：ALTER TABLE SET LOCATION不会自动更新已登记分区的LOCATION，必须逐分区更新并REFRESH TABLE；SQL目录用未转义的绝对路径，不能把“=”误写为“%3D”。实验Metastore与正式发布catalog隔离，切换是单用户串行操作，不提供多表原子切换；该目录不能给多个Spark进程同时使用。Derby/单节点文件路径仅用于教学，不作为生产服务。外部表分区删除在本次固定版本实验中保留源文件，脚本前后逐文件核验；不要把这一结论套用到其他表类型或purge设置。


## 付款时间回归

V10 将付款时间统一为数据库毫秒时钟；付款 Outbox 复制订单 pay_time，enqueued_at 保留入队时刻。已有历史不会自动纠正。先在隔离 hmdp_optimization_test 库运行 PaymentClockIntegrationTest（测试业务写入回滚），再运行下方 Spark 验收：

```powershell
docker --context desktop-linux compose -f dev/local/compose.yml --profile test up -d --pull never --wait mysql-test redis-test
docker --context desktop-linux compose -f dev/local/compose.yml run --rm --no-deps test mvn -o -B '-DargLine=-Xmx512m -Duser.timezone=UTC' '-Dtest=MockPaymentTest,PaymentClockIntegrationTest,MapperArchitectureTest' test
target/warehouse-venv/Scripts/python.exe -X utf8 data-platform/tests/acceptance_payment_time.py --tag my-new-clock-check
```

Java 验证真实服务付款、幂等 Outbox、事件投影和 UTC SQL 文本导出；Spark 验证导出的业务记录，以及同一瞬间的不同偏移表示、2秒告警、60秒/8小时/跨业务日/缺失时间阻断及失败保留旧指针。这是隔离事务内合成订单的实际服务链路，不是生产支付或独立只读快照抽取器的端到端证明。详情见 [时间契约](contracts/README.md)。


## 模型与Hive验收边界修复

当前维快照不等于历史维；连续日历范围、UTC timestamp和空Parquet输出契约见contracts/README.md。对旧版本验收时须注意：旧产物的字符串 _snapshot_as_of 和观测日期集合不能满足新物理契约，应保留旧版本、使用新run-id重建，不能在原版本上改文件。

新增3笔边界夹具，独立人工答案集中在expected.json。它验证未知金额付款、仅关单报表日、未投影创建日、连续日历、全部模型timestamp类型与UTC值、可读取的空结果；随后用Hive建表与独立进程重开核对分区和金额：

```powershell
target/warehouse-venv/Scripts/python.exe -X utf8 data-platform/tests/acceptance_model_edges.py --tag my-new-model-edges
```

Hive裁剪证据同时要求目标日期等值谓词和只包含目标日期的扫描位置，保存原Explain供复核；解析仅针对固定Spark 3.5.7格式，升级必须重验。reopen不再要求previous-version；Derby日志写入独立catalog目录，不改变Python工作目录。当前仍仅注册2个DWD表，DIM/DWS/ADS没有接入Hive，不能称为完整Hive数仓层。

## HDFS 最小实验（真实服务验收已通过）

独立 [Compose](../dev/local/hdfs.compose.yml) 配置一个 NameNode、一个 DataNode，副本数1；两个服务各限768MiB/0.5 CPU，Spark客户端限2GiB/2 CPU。没有宿主端口、Kerberos、权限隔离或HA，仅适用于本机可信Docker网络。两个持久卷分别保存元数据和块数据，不使用Windows目录存放HDFS块。当前Docker数据盘链接到D盘，具体证据与资源边界见 [部署核查](../docs/Hive-HDFS-OLAP的D盘部署核查.md)。

2026-11-09已补齐并核验`apache/hadoop:3.4.2`，**真实NameNode/DataNode服务验收通过**。此前下载失败记录仍保留作历史证据；当前镜像为linux/amd64，服务端Hadoop 3.4.2，Spark客户端Hadoop 3.3.4，固定摘要如下：

| 类型 | SHA256 |
| --- | --- |
| linux/amd64 manifest / RepoDigest | `0f79c18d27a2b15984868a41e245b09c98daa5638b573b4dbd848be650f05635` |
| image config digest（镜像存储Id也可能显示manifest digest） | `3a8752e6b7603e5da5967212315849a26c85bd416f056abe1702b12919cced60` |

可在网络可用处按摘要下载，并打上本地标签；也可在另一台机器`docker save`后在本机`docker load`。控制脚本会核对平台和固定image ID，错误标签不能绕过检查：

```powershell
docker --context desktop-linux pull --platform linux/amd64 apache/hadoop@sha256:0f79c18d27a2b15984868a41e245b09c98daa5638b573b4dbd848be650f05635
docker --context desktop-linux tag apache/hadoop@sha256:0f79c18d27a2b15984868a41e245b09c98daa5638b573b4dbd848be650f05635 apache/hadoop:3.4.2
python -X utf8 data-platform/tools/hdfs_control.py image
```

补齐后顺序执行以下步骤，每次验收tag换新名称。首次初始化必须显式`init`；`up`和`restart`绝不自动格式化，已有元数据或非空异常目录会受到保护：

```powershell
python -X utf8 data-platform/tools/hdfs_control.py init
python -X utf8 data-platform/tools/hdfs_control.py up
python -X utf8 data-platform/tools/hdfs_control.py status

# 若没有最新契约的合成s2/s3版本，先顺序生成；已有有效版本可按实际路径替换。
python -X utf8 data-platform/tools/warehouse.py run --input-root target/warehouse-fixtures/hmdp_hand_v1/s2 --source-batch s2 --biz-date 2026-10-30 --run-id hdfs-source-s2 --run-root target/hdfs-source-runs --cache-policy memory-and-disk
python -X utf8 data-platform/tools/warehouse.py run --input-root target/warehouse-fixtures/hmdp_hand_v1/s3 --source-batch s3 --biz-date 2026-10-30 --run-id hdfs-source-s3 --run-root target/hdfs-source-runs --cache-policy memory-and-disk
target/warehouse-venv/Scripts/python.exe -X utf8 data-platform/tests/acceptance_hdfs.py --tag my-hdfs-test --run-root target/hdfs-source-runs --previous-version target/hdfs-source-runs/versions/2026-10-30/hdfs-source-s2

python -X utf8 data-platform/tools/hdfs_control.py down
```

[验收脚本](tests/acceptance_hdfs.py) 只接收完整发布、带`hmdp-model-v2`标记的合成s2/s3输入；清单/产物哈希全部核对后，仅镜像两个DWD目录到独立HDFS路径，每批合计最多64MiB，读回每个文件校验SHA256后才rename上传目录。此rename只完成实验源镜像，不是正式数仓发布协议。Spark必须通过`hdfs://namenode:8020`读取，注册两个批次各两张Hive SerDe分区外部表；核对人工答案的逐店创建/付款金额和数量、未知金额、免费数、每日关单及独立事件日期，再检查裁剪证据并写入/读回付款笔数Parquet。

脚本随后重复`init`验证保留元数据，停止并重新启动本实验两个HDFS服务，核对clusterID、namespaceID、DataNode UUID；第二个Spark进程重开同一个本地Derby，重新校验HDFS哈希、表结果及写回结果，并在重启前后运行非空健康fsck。只有全部通过才写`acceptance.json`，失败留`failure.json`和日志，不自动删除失败目录/卷。脚本要求运行前HDFS已就绪，完成后保持服务原来的运行状态；上例最后手动`down`仅移除本实验容器/网络，保留命名卷，禁止加`-v`。同一实验集群、Derby目录一次仅允许一个验收进程。

输出位于`target/warehouse-hdfs-acceptance/<tag>`，代码、输入、日志/计划/报告都有哈希。本次实测报告为`target/warehouse-hdfs-acceptance/hdfs-20261109-v1/acceptance.json`：两批各12个DWD文件，HDFS文件逐个读回SHA256一致；s2为14订单/23事件，s3为14订单/25事件，10月30日付款由5笔15000分修订为6笔16000分；两批均完成Hive外部表、分区裁剪和HDFS Parquet写回。重复init未重新格式化，重启后clusterID、NameNode namespaceID、DataNode UUID、fsck健康结果和第二个Spark进程查询结果保持一致，源产物哈希未改变。验收后已停止HDFS容器，命名卷保留。

DIM/DWS/ADS、Airflow主链仍使用原本地版本目录；HDFS发布指针、并发补数、独立Metastore/HiveServer2和OLAP均未接入。此阶段只验证两张DWD的独立HDFS镜像与读写链路，不宣称全层迁移或生产级HA。
