# 本地离线调度

当前状态：DAG、受约束的文件任务桥接、宿主worker和CLI已实现；2026-11-02已通过Airflow 2.11.0真实scheduler验收，覆盖DAG导入、两个业务日、首次故障后的自动重试，以及迟到数据修订。当前范围是手动参数化的本地调度演示，不是自动日更或生产集群。

2026-11-02已通过16项契约/样本/桥接测试、Compose静态校验，以及真实Spark桥接验收：10月29日s1、10月30日s2、s3首尝试故障保留旧版本、第二次新run-id成功修正旧日报5笔/15000分→6笔/16000分，同尝试不会重复执行。机器报告为target/warehouse-bridge-acceptance/bridge-20261102/report.json，明确bridge_passed=true、airflow_scheduler_verified=false。

随后真实Airflow验收报告为target/warehouse-airflow-acceptance/airflow-20261102-v2/report.json，passed=true、airflow_scheduler_verified=true，完成于北京时间17:45:36。实际观察到首次尝试running→up_for_retry、try_number=2重新queued/running→success；首次故障期间旧catalog字节与旧产物均未改变，t2发布新版本并与独立手算答案核验一致。报告保存任务状态时间线、镜像/源码/产物哈希和日志证据。此前桥接报告保留为独立的较小验收范围，不改写历史结论。

## 运行结构

Airflow 2.11.0/Python3.11在独立Linux容器中运行，使用SequentialExecutor/SQLite，仅用于单用户本地教学。它将参数写入项目target任务目录，由宿主Python worker串行调用现有Spark Compose入口。Airflow不需要Docker CLI、Docker socket、宿主全局安装或宿主回连端口。scheduler配置上限1 CPU/1.5GiB，Spark仍是2 CPU/2GiB；请勿同时开启完整broker/RAG等其他重型服务。该版本为本次固定实验依赖，不是生产版本支持承诺。

每次DAG触发必须显式给biz_date、冻结source_batch及输入/输出目录；biz_date是报表日期，不从运行时“今天”推断，也不把Airflow触发时间伪装成历史数据库快照。DAG schedule=None/catchup=False，通过参数触发日任务或补数，不宣称已建立自动日更与自动抽取选批策略。

## 镜像与启动

唯一新增下载项为 `apache/airflow:2.11.0-python3.11`（linux/amd64）。现有Spark镜像复用，无需下载Hive/HDFS/PostgreSQL/Redis或provider插件；DAG使用镜像内置PythonOperator。固定Compose为 [airflow.compose.yml](../../dev/local/airflow.compose.yml)，离线启动使用pull=never，缺镜像时明确失败。

```powershell
docker --context desktop-linux pull apache/airflow:2.11.0-python3.11
docker --context desktop-linux image inspect apache/airflow:2.11.0-python3.11 --format '{{.Id}} {{.Os}}/{{.Architecture}}'
python -X utf8 data-platform/tools/fixture.py check
python -X utf8 data-platform/tools/airflow_control.py up
docker --context desktop-linux compose -f dev/local/airflow.compose.yml exec -T scheduler airflow dags list-import-errors -o json
```

元数据库与Airflow日志位于target/airflow-state，首次up会db migrate；配置不发布Web端口。本地控制台即可触发和看状态，未加入Web用户管理/展示看板。SQLite/SequentialExecutor不是生产调度基础设施；后续如需多用户、并行或看板，应单独设计PostgreSQL、认证和资源配置。

## 触发和补数

在一个终端启动worker，在另一个终端触发DAG：

```powershell
python -X utf8 data-platform/tools/orchestrate.py --idle-timeout 900
```

```powershell
python -X utf8 data-platform/tools/airflow_control.py trigger --config data-platform/orchestration/example-conf.json --run-id my-s2-run
python -X utf8 data-platform/tools/airflow_control.py status
python -X utf8 data-platform/tools/airflow_control.py logs
```

补数时将示例JSON复制到target，source_batch/input_root换成s3，biz_date仍为2026-10-30、run_root保持相同，使用新Airflow run-id触发。它会重建原日期而不是把迟到付款记到任务运行日。示例JSON没有密码，触发CLI通过结构化subprocess传递JSON，无shell拼接。

Airflow任务失败后重试一次，等待10秒；run-id由Airflow运行标识哈希和try_number组成，每次尝试新版本，旧失败stage保留。demo_fail_first=true仅在第一次尝试注入发布前故障，用于验证旧catalog保护；演示结束改回false。调度重试从冻结输入重新运行，不能当成精确一次执行。

## 协议与故障定位

target/warehouse-orchestration/queue保存request、claim、result JSON。源校验、输出目录、日期、cache_policy均受白名单约束，不能提交任意命令。只有project target下的相对路径可用；该共享目录是本地可信代码边界，不对不可信用户开放。任务15分钟未开始会过期；Airflow等待worker最多16分钟，task execution_timeout18分钟。Spark现有同业务日锁和质量门禁继续生效。

单个host-worker锁保证桥接任务串行，已有claim或result的尝试不会自动执行第二次。claim强杀后保留，worker锁或数仓锁也可能遗留；先确认没有相应进程/容器和仍在写的stage，再人工处理，并以新尝试重跑，不能盲目删锁。超时不会自动杀掉宿主Spark容器，也不自动回滚已经发布的版本；恢复前先查真实状态。

成功result记录不可变版本路径及manifest SHA256；Airflow再次验证该版本全部产物哈希，而不是从可能已被后续补数替换的catalog读取错误版本。XCom只返回版本标识/哈希。详细Spark日志在target/warehouse-logs/<attempt>.log，Airflow日志在target/airflow-state/logs，异常时保留旧成功版本。

## 验收边界与收尾

标准库测试可直接执行 `python -m unittest discover -s data-platform/tests -v`。不依赖Airflow的真实Spark桥接验收：

```powershell
target/warehouse-venv/Scripts/python.exe -X utf8 data-platform/tests/acceptance_bridge.py --tag my-bridge-test
```

它覆盖跨日期s1/s2、首次失败保留旧catalog、第二次新run-id成功、同尝试不重复执行、s3迟到修订；该桥接报告明确airflow_scheduler_verified=false。

完整Airflow验收先启动scheduler，再执行以下入口；无需另开worker，脚本会启动专用worker，串行执行4次Spark尝试并等待worker自然退出：

```powershell
python -X utf8 data-platform/tools/airflow_control.py up
target/warehouse-venv/Scripts/python.exe -X utf8 data-platform/tests/acceptance_airflow.py --tag my-unique-airflow-test
```

每次换新tag；验收拒绝已有host-worker锁或未完成的队列请求，避免混用其他任务。脚本通过真实DAG触发入口运行s1、s2、s3故障注入与自动重试；读取容器内SQLite的任务状态，全部只读查询，不直接写元数据库。Windows宿主读取运行中的Linux SQLite可能遇到日志恢复/锁兼容问题，因此状态观察放在容器内。首轮观察器失败的诊断和成功产物保留在target/warehouse-airflow-acceptance/airflow-20261102；修正观察位置后以新tag完成整套验收，没有清理或覆盖旧版本。

报告与宿主worker/scheduler日志位于target/warehouse-airflow-acceptance/<tag>。失败时先确认worker和Spark是否仍在运行；脚本不会强杀可能正在发布的worker，也不会自动删除锁。验收脚本不停止scheduler，便于查看状态；收尾时执行下方down。

```powershell
python -X utf8 data-platform/tools/airflow_control.py down
```

关闭后保留target中的元数据、日志、队列与数仓版本；停止宿主worker后再收尾。不自动清理原始冻结批次或唯一补数来源。

官方参考：[Airflow Docker](https://airflow.apache.org/docs/apache-airflow/2.11.0/howto/docker-compose/index.html)、[DAG Runs与catchup](https://airflow.apache.org/docs/apache-airflow/2.11.0/core-concepts/dag-run.html)。本轮运行结论来自固定版本镜像及实际状态、日志和业务产物验证；升级镜像后需重新执行验收。


## 并发与重试验收口径

当前是**单并发、阻塞式等待**：SequentialExecutor、parallelism=1、DAG max_active_runs=1；PythonOperator等待宿主worker最多16分钟，期间占用唯一执行槽位。两个dag_run同时触发时后者排队，这不是并行调度能力验证。若后续扩展，需要独立评估元数据库、Executor、资源和可延迟等待机制。

自动重试验收使用固定Airflow 2.11元数据库中持久化task_fail记录、最终第二次尝试成功，以及第一/第二次worker结果。worker结果保留执行前后catalog哈希，用于证明故障注入没有替换旧指针；旧版本文件另独立核验。1秒轮询是否看见up_for_retry只记录为辅助信息，不再决定验收成败。此证据协议适用于本地串行、可信共享目录，不提供第三方并发发布的事务保证。

read_success继续校验本次不可变版本，而不要求当前catalog仍指向它。后续合法补数替换最新指针，不会抹去先前任务成功事实；回归测试覆盖这一语义。若业务需要消费时确认最新版本，应另设计版本确认/协调协议，不能靠重复读取指针解决所有竞态。


2026-11-05真实scheduler回归通过：s1/s2首尝试成功，s3第一尝试故障、第二尝试成功，task_fail持久化失败数为1，retry_verified_from_persisted_evidence=true，旧指针及不可变产物保护通过。报告target/warehouse-airflow-acceptance/airflow-review-20261105/report.json；本轮已停止scheduler，保留元数据和日志。
