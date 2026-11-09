# Hive / HDFS / OLAP 的 D 盘部署核查

核查：2026-10-31（北京时间）。本轮只核实安装位置与资源可行性，没有下载/启动这些新组件，也没有迁移 Docker 或修改现有链接。

> 2026-11-05更新：已复核Docker主要数据盘仍链接至D:/DockerData/wsl/disk/docker_data.vhdx。当前约C盘13.1GiB、D盘45.3GiB可用，Docker VM约7.36GiB；下文10月31日数字保留为历史快照。已完成Spark内嵌Hive Metastore + 本地Parquet外部表实验，详细范围见本文末尾；尚未安装HDFS、独立Hive服务或OLAP。

## 结论

**可以放在 D 盘。并且当前 Docker 的主要数据磁盘已经位于 D 盘。** 不需要为本项目再做一次全局 Docker 迁移，也不应仅因 C 盘空间少就判断不能安装。

但磁盘位置和运行资源是两件事。本机 D 盘约余38.6GiB、Docker VM 内存上限约7.36GiB，适合少量样本、单机组件错峰学习，不适合把业务、Spark、HiveServer2、Metastore、HDFS、OLAP 与 RAGFlow 一起常驻。还不能仅凭“D盘可用”宣称整套环境安装验收通过。

## 实际证据

| 项目 | 本机读到的结果 |
| --- | --- |
| C 盘 | 约8.7GiB空闲 |
| D 盘 | NTFS，Healthy，总约329GiB，约38.6GiB空闲 |
| C盘Docker入口 | `C:\Users\86150\AppData\Local\Docker\wsl\disk\docker_data.vhdx` 为 SymbolicLink，文件自身 Length=0 |
| 实际链接目标 | `D:\DockerData\wsl\disk\docker_data.vhdx`，目标存在，文件逻辑长度约38.16GiB |
| Docker内部根目录 | `/var/lib/docker`，Engine 29.3.1，Linux/WSL2；启动现有BusyBox并读取文件系统成功 |
| 容器 df | overlay 总约1006.9G、已用37.3G、可用918.4G；**这是虚拟文件系统容量，不能替代D盘物理剩余38.6GiB** |
| Docker VM内存 | 7,898,071,040字节，约7.36GiB；宿主约16GB |

注意：38.16GiB是已存在文件长度，已经包含在D盘已用空间中，不应从“空闲38.6GiB”里再扣一次。镜像解压、数据复制、快照备份和后续文件增长才继续消耗空闲。

上轮复核记录对磁盘位置采用了当时的C盘假设，本次通过符号链接目标修正。当前没有执行迁移，无法据此判断迁移由谁、何时完成。链接方式本身不是本轮实施方案，不建议在系统运行时手工搬移或删除VHDX。

## 三类组件怎样落到D盘

| 组件 | 数据位置与建议方式 | 单机边界 |
| --- | --- | --- |
| HDFS | Linux容器镜像与NameNode/DataNode命名卷随当前Docker磁盘落在D盘VHDX；配置与源码保留项目目录 | 单NameNode/单DataNode、复制因子1，仅教学，无HA/副本容灾；HDFS里仍存Linux路径，不写D盘盘符 |
| Hive | Hive Metastore进程、外部元数据库及warehouse数据均可使用Docker命名卷；数据放HDFS或专用warehouse卷 | Hive不是一个“数据库目录”；要区分元数据服务、查询服务和执行引擎。Spark只接Metastore时可先不常驻HiveServer2 |
| OLAP | 选择一个引擎；例如ClickHouse容器的`/var/lib/clickhouse`使用专用命名卷，同样落在D盘 | Doris/StarRocks通常还涉及FE/BE等进程，版本与资源要求须另核；“OLAP”不是一个可一次装齐的软件 |

也可把普通交换文件绑定挂载到项目下 `data/` 或 D盘专用目录。数据库和HDFS底层块数据优先留Linux命名卷，避免大量小文件经Windows共享路径带来的权限/性能差异。**只把Compose文件放D盘，并不能保证镜像或数据卷就在D盘；本机能判断落D盘，是因为已核实Docker实际数据磁盘。**

宿主安装器、VS Code缓存、Docker日志/少量系统组件仍可能写C盘；将主要数据放D盘不代表C盘零增长。当前链接保持原状，后续Docker升级/重新配置后应再次核对目标。

## 空间与内存预算

下面是小样本实验的规划估算，未做这些组件的实际拉取/压测，不是官方最低配置或性能承诺：

- 当前D盘空闲约38.6GiB，可将首轮新增镜像和小数据总增量限制在约15～20GiB，预留至少约15GiB供解压、临时文件与系统正常使用；镜像精确大小须选定版本后核实。不要三套全量同时下载后才检查空间。
- Hadoop单节点+Hive Metastore先控制在约2～3GiB服务内存预算，Spark容器继续上限2GiB；若增加HiveServer2/执行进程，要重新计总账，不能假设无需额外资源。
- OLAP教学优先单个轻量实例，先预算约1～2GiB并测峰值；如引擎所选版本需要更高内存，必须调整选择或升级内存。不能套用该估算到所有OLAP产品。
- 7.36GiB是Docker VM的总上限，不是每个容器都能占用；还需留给Linux、文件缓存、MySQL等。HDFS/Hive实验与OLAP实验错峰，停止无关项目；不能用D盘空间弥补内存不足。
- HDFS副本、ODS快照、Parquet派生、OLAP导入会重复保存同一数据。数据规模固定、显式保留周期；不能把上游5GiB直接当作整套系统只占5GiB。

因此，**D盘可以承载后续小规模安装，现阶段没有必须再次迁移Docker的阻塞；完整栈同时常驻的瓶颈仍在内存和增长空间。** 本轮先交付可手算样本，具体安装哪些组件等下一轮选型后再执行。

## 参考及核验边界

- [Apache Hadoop 单节点/伪分布式配置](https://hadoop.apache.org/docs/r3.4.2/hadoop-project-dist/hadoop-common/SingleCluster.html)：本轮实际读取成功，文档提供单机伪分布式和`dfs.replication=1`配置；不等于本机已部署。
- [Docker Desktop WSL2](https://docs.docker.com/desktop/features/wsl/) 与 [ClickHouse Docker安装说明](https://clickhouse.com/docs/install/docker)：供后续选型查阅。本轮访问分别遇到连接重置和403，未将其内容当作已核验的版本要求。

本机结论依据Get-Item的LinkType/Target、Get-Volume、Docker info和现有镜像只读df输出。没有删除镜像/数据卷，没有改动其他项目容器，也没有把预估资源写成实测。

## 2026-11-05内嵌Hive实验实际落盘

本次复用apache/spark:3.5.7-scala2.12-java17-python3-ubuntu镜像，不下载Hive/HDFS/OLAP镜像。镜像内实际存在hive-metastore-2.3.9.jar、derby-10.14.2.0.jar；容器上限2GiB、2 CPU、driver768m、local[2]。没有将内嵌Hive类库称为已部署HiveServer2或分布式Hive平台。

实验Metastore、warehouse目录、执行计划、报告与全部日志放在项目D盘的target/warehouse-hive-acceptance/<tag>/catalog。源Parquet仍引用target中经过质量门禁发布的不可变版本，切换目录只更新独立教学Metastore。两次Spark进程顺序启动并复用同一Derby目录，已验证重启可读；不支持多进程并发访问该目录。

已验证Hive SerDe分区外部表、分区元数据移除/恢复、分区裁剪，以及s2→s3迟到修订；源文件哈希前后相同。未建立HDFS块目录、独立外部Metastore数据库或OLAP导入副本，相关资源预算仍是规划，不是本阶段运行结论。

本轮接手时openmaic-dev-db-postgres-1运行，保留原状；未停止或删除其他项目服务、数据卷。收尾仅清理hmdp-local实验网络。部署与复现入口见 [模块说明](../data-platform/README.md)。

## 2026-11-05～2026-11-09 HDFS部署与实测

本轮新增独立hmdp-hdfs项目：NameNode/DataNode各限768MiB、0.5 CPU，分别使用namenode-data/datanode-data命名卷；Spark客户端限2GiB、2 CPU、driver768m。服务加客户端内存上限合计3.5GiB，另需预留VM系统、缓存、其他容器和宿主内存；这些是配置预算，不是实测峰值。小样本错峰运行，不与Airflow/业务全栈/OLAP同时常驻。

部署前核查快照：Docker VM约7.36GiB，宿主可用约2.4GiB，C盘空闲约3.5GiB、D盘约45.1GiB；空间随其他任务变化，上文历史数值不代表当前余量。主要Docker VHDX仍链接D:/DockerData/wsl/disk/docker_data.vhdx。本配置将元数据和块数据放Linux命名卷，按现有VHDX位置承载在D盘；Derby、配置、源Parquet与日志在项目D盘target目录。不能把Linux卷Mountpoint或容器df的虚拟容量当作D盘物理剩余空间。

镜像固定apache/hadoop:3.4.2 linux/amd64，manifest/RepoDigest为`sha256:0f79c18d27a2b15984868a41e245b09c98daa5638b573b4dbd848be650f05635`，config摘要为`sha256:3a8752e6b7603e5da5967212315849a26c85bd416f056abe1702b12919cced60`。用户补齐后已实际启动并验收NameNode/DataNode；服务端Hadoop 3.4.2，Spark客户端Hadoop 3.3.4，完整证据见`target/warehouse-hdfs-acceptance/hdfs-20261109-v1/acceptance.json`。未调整全局代理、未迁移VHDX、未新增OLAP或独立Hive服务。

本轮实际验证Compose配置、镜像布局、HDFS安全模式退出和DataNode注册、Spark 3.5.7内置Hadoop 3.3.4客户端连接Hadoop 3.4.2服务端、两批DWD文件读写、Hive外部表、Parquet写回、重启持久性和fsck健康。重复init未格式化，NameNode namespaceID与DataNode UUID保持不变。单节点/副本1、simple认证和关闭HDFS权限仅限可信本机教学网络，不提供HA或容灾能力；验收后服务已停止，命名卷仍保留。
