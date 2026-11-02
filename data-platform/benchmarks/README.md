# 本地 SQL 与性能实验

这是有正确性门槛的本地实验，不是集群容量报告。使用现有 Spark 3.5.7 容器，无需 MySQL、Hive、HDFS 或新增镜像。每次最多一个 Spark 容器：2 CPU、2GiB 内存，driver 768m，local[2]。宿主 Windows/WSL2 与挂载文件系统的开销包含在观测中。

## 2026-11-02 实测

两档规模均通过独立源文件/PyArrow对账，缓存前后全部10个输出模型逻辑相同。便于随源码审阅的 [结果摘要](results-20261102.json) 包含重复测量、SQL文件/Shuffle计数和Explain哈希；原始完整证据保存在本机 `target/warehouse-scale/reports/perf-20261102-v2/`。

| 完整作业规模 | 无缓存进程耗时 | MEMORY_AND_DISK进程耗时 | 无缓存/缓存的Spark工作段 |
| --- | ---: | ---: | ---: |
| 1万订单 / 17913事件 | 53.66s | 45.21s | 45.22s / 36.92s |
| 10万订单 / 179112事件 | 108.27s | 68.53s | 84.88s / 44.88s |

完整作业各策略各规模仅一次；“进程耗时”包括容器/Spark启动、Python源校验和收尾，不含样本生成与独立结果核验。单项SQL中位数如下，每格各3次：

| 查询对照（基线→候选） | 1万订单 | 10万订单 |
| --- | ---: | ---: |
| 时间推导过滤→业务日期分区过滤 | 0.451s→0.294s | 0.546s→0.394s |
| 强制SortMerge→强制Broadcast | 0.302s→0.511s | 0.298s→0.490s |
| 每日32个小文件→每日2个文件 | 0.832s→0.128s | 0.798s→0.141s |
| 普通聚合→加盐两级聚合 | 0.377s→0.399s | 0.372s→0.417s |

日期过滤实际文件扫描从10个变1个；小文件对照从32个变2个。10万规模两种布局写入分别耗时73.17s和4.76s，总文件320和20；重写成本需与查询频次一起评估，不能只报查询收益。按店铺分区时最大分区占83.632%的行，但普通聚合已有局部合并，加盐额外Shuffle在本次数据下没有收益。广播也没有更快，且绝对耗时不足1秒、受调度/JIT/缓存波动影响，因此不向主链强加广播或加盐hint。保留Spark默认AQE/自动广播设置，用明确分区条件查询已发布DWD，并按需启用中间结果缓存。

10万规模源文件约126MiB；该规模证明本机正确性与执行机制，不能据此估算生产吞吐、千万级作业或分布式伸缩。

## 复现

在项目根目录的 PowerShell 执行，先确保 Docker Desktop 运行、已安装模块所需依赖：

```powershell
target/warehouse-venv/Scripts/python.exe -m pip install -r data-platform/requirements-verify.txt
target/warehouse-venv/Scripts/python.exe -X utf8 data-platform/tools/performance.py --tag my-unique-perf-run
# 只跑1万订单可增加 --sizes 10000；tag每次换新，不覆盖旧版本。
```

生成和运行内容均在 `target/warehouse-scale/`，不写业务库，不上传原始数据、Parquet、虚拟环境与日志。生成器上限10万订单，默认依次执行1万和10万；原始文件有SHA256，重复生成时校验冻结文件，损坏或半成品需使用新目录，不能直接覆盖。首次生成和已存在输入的校验时间都独立记录，不计作作业收益。产物应按需自行保留；入口不删除历史版本。

## 数据与正确性

固定seed=42，通过整数映射生成100家店、200种券、10个创建日；约80%的订单集中于店铺1，包含跨午夜付款、未知金额、免费订单、取消与待付。源ID超过JavaScript安全整数范围，全程字符串，金额为整数分。金额与当前券价刻意不同，防止用现价替代历史快照。情景截点只是生成参数，不代表曾保存过真实历史数据库。

每个规模执行原始主链与可选 `memory-and-disk` 缓存主链，均运行全部质量门禁。独立Python校验器读取序列化源文件，以字典/集合聚合，与PyArrow读取的DWS全部数量、金额、去重人数、未知/免费、付款与关单，以及券cohort、排行、状态、DWD金额/ID/行数核验；不调用Spark模型SQL。再比较缓存前后所有DIM/DWD/DWS/ADS的排序逻辑内容。未知金额组的SUM保留NULL，缺失的指标组为0。

## 对照方法

完整主链每策略每规模各一次，1万先无缓存、10万先缓存，减少固定顺序偏差。记录进程总时间与作业内的源/模型准备、质量、输出时间。没有清空OS/Docker文件缓存，不把单次差值视为统计意义上的稳定加速。

单项SQL对照均运行在已验证的DWD Parquet上：每个变体先预热一次，再按AB、BA、AB顺序各测3次，以collect触发实际执行，保存每次时间、Explain和执行后SQL metrics。每次结果必须逐行一致，否则停止验收。为显式观察Join计划，实验关闭AQE和自动广播、shuffle=4；这与主链默认AQE开启、自动广播10MiB、shuffle=2不同，不能把实验差值直接当主链收益。

| 对照 | 实际变化与观察范围 |
| --- | --- |
| 日期分区 | 从创建时间推导日期过滤，对照直接过滤biz_date；后者可分区裁剪，也少读时间列，耗时同时受二者影响 |
| 小维表关联 | 强制SortMergeJoin对照BroadcastHashJoin；核查计划节点，不假定广播一定更快 |
| 小文件 | 相同DWD按32/2路重分布后写入日期分区，分别查询；保留重写时间、文件数、字节数，不隐藏合并成本 |
| 热点聚合 | 普通部分聚合对照8盐值的两级聚合；先保存按店铺分区后的行数分布，行数偏斜不等于任务时间倾斜 |
| 列裁剪 | 保存只读shop_id/pay_cents与读取全部列的ReadSchema；两者计算不同，不做误导性的耗时比值 |

输出 `reports/<tag>/report.json`、每规模的 `queries-n*/report.json`、计划文本、进度和日志。SQL metrics包含各节点的文件/输出/Shuffle/耗时等可用计数；其中filesSize是选中文件总大小，不是精确物理IO或解压后字节数。布局文件仅服务本次对照，不替换正式数仓版本。

需要导出轻量摘要时，使用新文件名执行：

```powershell
python -X utf8 data-platform/tools/performance_summary.py --report target/warehouse-scale/reports/my-unique-perf-run/report.json --output target/warehouse-scale/summary-my-run.json
```

## 使用缓存

```powershell
python -X utf8 data-platform/tools/warehouse.py run --input-root target/warehouse-scale/sources/scale-v1-n100000-s42 --source-batch scale-v1-n100000-s42 --biz-date 2026-10-30 --run-id cached-example --cache-policy memory-and-disk
```

缓存三个DWD与一个DWS，使用MEMORY_AND_DISK复用重复质量检查和输出的中间计算；内存不足时允许落盘，失败仍遵守原有发布协议。默认策略保留none，通过参数显式选择，不在未知资源与数据规模下强行缓存。每次发布的 `execution.json` 记录策略、资源配置和阶段耗时，也纳入产物哈希。

本次缓存路径还重新通过原14笔样本的s1/s2/s3、迟到修订、重跑和发布前失败保护。可用独立tag复现：

```powershell
target/warehouse-venv/Scripts/python.exe -X utf8 data-platform/tests/acceptance.py --tag my-cache-regression --run-root target/warehouse-cache-acceptance --cache-policy memory-and-disk
```
