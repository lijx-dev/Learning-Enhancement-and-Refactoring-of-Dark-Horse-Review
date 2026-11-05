# Airflow调度阶段待下载清单

更新：2026-11-02。该下载清单已完成，保留为历史交接记录，无需重复下载。用户补齐镜像并恢复Docker后，已核验linux/amd64、Airflow 2.11.0；scheduler健康且项目DAG导入无错误。后续运行验收结果见 [分阶段实施记录](离线数仓分阶段实施记录.md)。

| 项目 | 精确要求 | 本机状态 |
| --- | --- | --- |
| Docker镜像 | apache/airflow:2.11.0-python3.11，linux/amd64 | 已下载并核验版本、架构 |

下载前取得的清单：多架构index摘要 `sha256:6126ada380e94b4c0442fe72fee20d1d59e1ad10ff14398c4b75a606646ad26e`，linux/amd64 manifest摘要 `sha256:cbb7764177abfc8366c37fc13c415bc6a9514fcdc9be1c5da6a60cd1cddbf898`。该清单21层压缩合计501784081字节（约479MiB）；实际下载/解包空间更多。实际Docker image inspect的Id/RepoDigests均记录为前述index摘要，且容器内airflow version返回2.11.0；不把index摘要误称为单平台config摘要。

历史网络失败依据：Docker沿用127.0.0.1:7897代理但未监听；官方registry宿主直连超时；docker.m.daocloud.io取得manifest后，镜像层持续403。此前已停止下载重试，没有开启代理或改写全局网络设置。

## 你在本机下载

在可用网络/代理配置下执行：

```powershell
docker --context desktop-linux pull --platform linux/amd64 apache/airflow:2.11.0-python3.11
docker --context desktop-linux image inspect apache/airflow:2.11.0-python3.11 --format '{{.Id}} {{.Os}}/{{.Architecture}}'
docker --context desktop-linux run --rm --entrypoint airflow apache/airflow:2.11.0-python3.11 version
```

以下命令保留供重新安装参考，当前机器无需重复执行。结果应为linux/amd64、Airflow 2.11.0。

## 在其他电脑下载后转移

```powershell
docker pull --platform linux/amd64 apache/airflow:2.11.0-python3.11
docker save -o airflow-2.11.0-python3.11-amd64.tar apache/airflow:2.11.0-python3.11
```

复制tar到本机D盘，再执行（按真实路径修改）：

```powershell
docker --context desktop-linux load -i D:/Downloads/airflow-2.11.0-python3.11-amd64.tar
```

不用下载Hive、HDFS、Kafka、Airflow provider扩展、PostgreSQL或Windows版Airflow；现有Spark镜像与Python环境复用。不要pip安装Airflow到宿主全局Python，也不要为此删除其他项目镜像/卷。

真实Airflow运行范围与验收状态以 [调度模块说明](../data-platform/orchestration/README.md) 为准；本文件作为个人下载交接资料留本地，不纳入项目提交。
