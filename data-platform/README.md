# 离线数据开发模块

已实现轻量主链：只读MySQL一致性抽取 → ODS冻结批次 → Spark SQL DIM/DWD/DWS/ADS → 质量门禁 → 版本发布。已用手算样本与真实MySQL读取验证，完成本机1万/10万合成订单的SQL性能实验，以及Airflow参数化运行、自动重试与迟到补数验收；没有分布式部署或生产吞吐结论。

- [数据契约](contracts/README.md)、[列白名单](contracts/source-contract.json)：粒度、金额、时区与未知历史。
- [可手算样本](fixtures/hand-calculable/README.md)：14笔合成订单、3个情景快照与独立人工答案。
- [分层SQL](sql/models.sql)：3个DIM、3个DWD、1个DWS、3个ADS。
- [SQL与性能实验](benchmarks/README.md)：1万/10万确定性样本、缓存对照、分区/广播/文件/热点聚合实验及复现入口。
- [本地调度](orchestration/README.md)：参数化DAG、受约束的宿主任务桥接；Airflow 2.11.0真实scheduler的按日运行、自动重试、旧版本保护及迟到补数已验收。
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

可通过 `--cache-policy memory-and-disk` 复用DWD/DWS中间计算，默认none；发布版本中的execution.json记录实际策略和阶段耗时。性能实验方法和验收结果见上方实验说明。调度当前是schedule=None的手动参数化触发，自动日更/抽取选批、按需Hive/HDFS/查询服务仍待建设；当前未验证退款完整事件、CDC增量删除、对象存储发布、硬故障自动恢复或生产支付。
