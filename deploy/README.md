# 阿里云初步体验部署：2GB 轻量版

本文已将原部署的主机地址、安全组标识和个人路径替换为示例。`203.0.113.10` 是文档保留地址，不是在线演示；请使用自己的服务器信息。

适用的已确认环境：Alibaba Cloud Linux 3、x86_64、约 2GB 内存、40GB 系统盘，Docker 26.1.3 / Compose 2.27.0。目标是先让别人通过浏览器体验当前点评项目。

## 1. 这套部署运行什么

运行 Nginx 桌面网页、Java、MySQL、Redis、Memcache。后端使用当前源码编译的可执行 JAR，前端使用当前 `frontend` 静态页面，所有网页请求仍走同域 `/api`。

登录、店铺/搜索、签到、社区、普通券与订单、缓存采样可以体验。秒杀需要管理员准备并发布活动，走 Redis Stream 和本地履约。持久 Outbox/Inbox 仍用于本地投影。免费券的“已领取”状态不表示真实收款，模拟支付默认关闭。

这台 2GB 机器先关闭 Kafka、RocketMQ、Canal。体验页会显示对应功能未启用；不能据此宣称已验证真实消息中间件。五层缓存实现保留，具体采样命中哪一层取决于当时状态。数据库直改不再由 CDC 自动传播，演示更新请使用业务接口。

限额用于约束容器和 JVM，并不保证任何负载都能运行在 2GB；首次启动和实际体验后需检查内存与 OOM。当前只做小范围试用，暂无真实短信、支付或多机容灾。

## 2. 本地打包，再上传服务器

本地 Mac 终端在 Java 项目根目录执行；这些命令不是在云端 SSH 中执行：

```bash
cd '/path/to/Learning-Enhancement-and-Refactoring-of-Dark-Horse-Review'
# 本机有 Maven 时使用；跳过业务测试，但保留已有构建检查
mvn -Dmaven.test.skip=true package
python3 deploy/package-release.py
```

本机没有 Maven 时，可使用已经存在的开发 Maven 缓存：

```bash
docker run --rm -v "$PWD:/workspace" \
  -v hm-dianping_maven-cache:/root/.m2 -w /workspace \
  maven:3.9-eclipse-temurin-8 mvn -Dmaven.test.skip=true package
python3 deploy/package-release.py
```

生成 `target/hmdp-cloud-release.tar.gz`。包内只有可执行 JAR、前端、初始数据库 SQL、可选初始图片、云端配置与校验清单，不包含 `.env`、SSH 密钥、IDE 状态或本机数据库备份。默认素材目录为仓库内 `dev/images`；没有原课程图片时显示前端占位图，也可以用 `--images-dir /path/to/demo-images` 指定自己的可再分发素材。它建立新的演示环境，不会把本机现有订单/用户/库存整体迁过去。

通过阿里云远程连接工具的文件上传功能，把这个包传到服务器 `/root`。也可在 Mac 终端使用：

```bash
scp target/hmdp-cloud-release.tar.gz root@203.0.113.10:/root/
```

当前云端登录工具生成的临时 SSH 密钥不一定被本机 scp 自动使用。需要额外认证时，使用你已有的文件上传功能或自己已有的 SSH 凭证；不必把密码/私钥发给助手。初次独立连接还要确认服务器主机指纹。

## 3. 服务器端解压和创建配置

以下在已经登录的云服务器终端中执行。首次安装到 `/opt/hmdp-cloud`，如目标目录已经有别的部署，先核对归属，不直接覆盖。

```bash
mkdir -p /opt/hmdp-cloud
tar -xzf /root/hmdp-cloud-release.tar.gz -C /opt/hmdp-cloud --strip-components=1
cd /opt/hmdp-cloud
sh init-env.sh
docker compose --env-file .env -f compose.yml config --quiet
```

`init-env.sh` 生成独立随机的数据库 root、业务数据库、Redis 和监控凭证，文件权限为 600；已有 `.env` 时不覆盖。后端以 `hmdp_app` 连接数据库，不使用 root。首次创建数据库卷时导入课程初始 SQL，随后 Java 自动执行已有 Flyway 增量迁移。

不要删除 `.env` 再生成密码后继续连接旧数据库卷：旧库密码不会因此自动更新。升级保留 `.env` 和三个数据卷。配置检查使用 `--quiet`，避免把展开的凭证输出后复制到聊天或分享材料中。

## 4. 构建并启动

```bash
cd /opt/hmdp-cloud
docker compose --env-file .env -f compose.yml up -d --build
docker compose --env-file .env -f compose.yml ps -a
docker compose --env-file .env -f compose.yml logs --tail=100 java
curl -i 'http://127.0.0.1:8080/api/shop-type/list'
```

构建只把预编译 JAR 放入 Java 8 运行镜像，云端无需重新运行 Maven。首次仍需下载基础镜像。若 Docker Hub 拉取失败，要使用你有权限的镜像仓库/加速配置，或通过可信本机 `docker save` 上传所需镜像；不要随意替换为来源不明的项目镜像。

`image-init` 的 Exited(0) 表示初始图片复制完成，是正常状态。复制不覆盖已有同路径图片，后续上传数据保存在 uploads 卷。web 的健康检查经过 `/api/actuator/health`，等待 Java 启动后才能 healthy。

网页端口为 `0.0.0.0:8080`；Java 8081 仅绑定服务器本机；MySQL、Redis、Memcache不发布宿主端口。公网访问通过 Nginx，管理运维 `/api/ops/` 和 Prometheus 路由不在该入口公开。Nginx 重写 X-Forwarded-For，Java按 native策略恢复访客地址，避免所有人被简单当成同一个代理 IP限流。

## 5. 阿里云安全组开放网页端口

在你自己的 ECS 实例中核对实际绑定的安全组；若入方向规则没有网页端口，进入 ECS 实例 → 安全组 → 入方向 → 添加规则：

| 字段 | 本次配置 |
| --- | --- |
| 协议 | TCP |
| 端口范围 | 8080/8080 |
| 授权对象 | 小范围试用填体验者公网 IP/CIDR；需要公开网页时才使用 0.0.0.0/0 |
| 描述 | 黑马点评网页体验 |

不需要开放 3306、6379、11211、8081或 MQ/Kafka端口。安全组放行与服务器防火墙是两层检查；如果 firewalld已启用，需在实际生效的 zone中放行网页端口，不直接关闭整个防火墙。

```bash
# 只读判断当前防火墙状态和生效 zone
systemctl is-active firewalld
firewall-cmd --get-active-zones
```

Docker的端口发布与防火墙规则还取决于实际网络配置。以安全组和从外部浏览器实际访问的结果为准，不将“本机 curl成功”当成公网已开放。

来源：[阿里云 Docker 部署说明](https://help.aliyun.com/zh/document_detail/2842585.html)、[Docker 端口发布](https://docs.docker.com/engine/network/port-publishing/)。

### 本机已遇到的容器互联问题

2026-10-11 云端首次启动时，Java 报 `No route to host`，不能连接已经健康的 Redis。实际检查发现 firewalld 正在运行，但点评的 Docker 桥接接口没有被关联到允许容器通信的 zone；iptables 的转发规则存在，firewalld 的另一组规则仍然拒绝了流量。这个错误发生在服务器内部，与阿里云是否放行公网 8080 是两个问题。

本次只将点评项目自己的桥接接口加入现有 `trusted` zone，并在绑定 eth0 的 `public` zone 放行 8080。没有关闭 firewalld，也没有开放数据库宿主端口。下列命令仅适用于相同诊断结果；正常通信的服务器无需照抄：

```bash
bridge_id=$(docker network inspect hmdp-cloud_default --format '{{.Id}}')
bridge_name="br-$(printf '%s' "$bridge_id" | cut -c1-12)"
ip link show "$bridge_name"
firewall-cmd --zone=trusted --add-interface="$bridge_name"
firewall-cmd --permanent --zone=trusted --add-interface="$bridge_name"
firewall-cmd --zone=public --add-port=8080/tcp
firewall-cmd --permanent --zone=public --add-port=8080/tcp
docker compose --env-file .env -f compose.yml restart java
```

`trusted` 对关联接口放行流量，所以只绑定已核对的点评桥接接口，不能把公网网卡 eth0 放进去。规则同时写入当前运行配置和永久配置，本次没有执行全局防火墙 reload。后续 `down` 会删除 Compose 网络，再次 `up` 时桥接接口可能改变，需重新核对并更新这条接口关联，移除不再使用的旧接口记录。不要把首次部署时的接口名称写死。

Docker 的标准 firewalld 集成会为 Docker 桥接接口创建专用 zone。换服务器或调整 Docker 配置时，应先检查实际集成状态，再决定是否需要上述兼容处理。参考：[Docker 与 firewalld 的集成](https://docs.docker.com/engine/network/packet-filtering-firewalls/#integration-with-firewalld)。

## 6. 最终访问与正常登录

网页示例：`http://203.0.113.10:8080/`，请替换为自己的服务器地址。

体验页示例：`http://203.0.113.10:8080/experience.html`。

先在自己的电脑浏览器打开，核对真实店铺数据、图片、筛选和缓存采样。课程店铺图片有第三方 URL，并非全部是本地文件；这些图片依赖外站可用性，页面已有加载失败占位图。本地分类、笔记图片和前端资源已打包。再让一位外部体验者打开，确认安全组范围和公网链路。

当前验证码不会真正发送到手机。试用者在登录页面点获取验证码后，部署者需要在服务器读取对应时段的 Java日志并提供验证码。现有调试日志不标注手机号，多人初步体验请按账号依次操作。不要公开整个日志，也不要共享自己的管理员 token。

```bash
docker compose --env-file .env -f compose.yml logs --since=2m java
```

在两分钟内用验证码完成登录；正常登录后可看本人资料、签到、领券、订单和发布笔记。公开体验不依赖登录的部分包括店铺、搜索和缓存观察。若希望访客自行完成登录，需要后续接入短信服务或另行实现明确的演示账号流程；本部署没有偷偷加入登录绕过。

当前 IP + HTTP入口适合有限的初步演示。正式长期分享前应配置域名与 HTTPS，保护登录凭证传输；涉及域名和服务器区域的备案要求，按阿里云当前规则另行确认。本配置未自动申请证书。

## 7. 管理员与模拟支付

先正常登录，在浏览器 Network 查看 `/api/user/me` 返回的真实 `data.id`。编辑 `/opt/hmdp-cloud/.env` 的 `HMDP_ADMIN_IDS`，多个 ID英文逗号分隔。需要模拟支付演示时设置 `HMDP_PAYMENT_MOCK_ENABLED=true`，不要将模拟交易解释为真实支付。

```bash
cd /opt/hmdp-cloud
vi .env
docker compose --env-file .env -f compose.yml up -d --no-deps java
```

重建容器才能更新环境变量，`restart` 不会重新读取 .env。管理员可准备商品；运维接口从服务器 localhost:8081或自己的 SSH隧道访问，仍要求业务管理员身份。

## 8. 内存、运行状态与日志

```bash
docker compose --env-file .env -f compose.yml ps -a
docker stats --no-stream
free -h
df -h /
docker compose --env-file .env -f compose.yml logs --tail=100 mysql java web
```

Java堆上限 256MB，容器上限512MB；MySQL上限512MB并缩小缓冲区，Redis上限192MB且禁用自动淘汰业务 Key，Memcache上限64MB，Nginx上限64MB。这是初始体验预算，应用的原生内存、线程、数据库初始化峰值和宿主进程都要算在内。

如果容器重启，检查是否 OOMKilled，不先用反复重启掩盖。Redis达到 maxmemory后会拒绝新增写入，不能为腾空间使用 FLUSHALL或随机删秒杀/签到数据。进一步并发演示、全部中间件或更大数据量需要重新评估资源，本文件不保证2GB可以承载所有亮点与压测。

日志轮转每容器最多3份10MB文件，避免长期运行的普通日志无界增长。数据库、Redis和图片仍是持久数据，需要按实际增长规划备份。

## 9. 日常停止与更新

```bash
# 保留数据卷与 .env
docker compose --env-file .env -f compose.yml down
# 继续使用现有数据
docker compose --env-file .env -f compose.yml up -d
```

不要在保存了体验数据的环境使用 `down -v`。更新前备份数据库、Redis持久数据、uploads与 .env；新包中的SQL只用于首次数据库卷初始化，不要手工再导入覆盖旧库。

重新编译打包后，将新包上传并更新运行文件，保留已有 .env和数据卷，再执行 `up -d --build`。这是就地升级，不是零停机滚动发布；Flyway执行结构迁移前应具备可恢复备份。

## 10. 部署验收记录与最小验收清单

2026-10-11：当前源码 Maven package成功，Checkstyle 0违规；Compose与 Nginx语法检查通过；随机凭证生成、600权限及重复调用不覆盖已验证。部署包使用独立 `hmdp-cloud-smoke` 项目做本地启动验收：数据库自动初始化/迁移、业务数据库账号、Redis密码、代理查询、静态图片、两次缓存读取、正常验证码登录、签到及公开运维路径拒绝均通过。

本地验收复用缓存 Maven/Temurin Java 8镜像执行相同 JAR。测试容器与临时数据已单独清理。

2026-10-11 13:50（北京时间）：部署包上传至 `101.132.47.134` 后，SHA-256 校验通过，已解压到 `/opt/hmdp-cloud`。云端成功下载默认 `eclipse-temurin:8-jre`，完成镜像构建、MySQL 初始化、应用启动和内部防火墙修复。通过服务器本机 Nginx 入口验证了首页、体验页、优惠券页、本地图片、分类、店铺列表/详情、两次缓存读取、正常验证码登录、本人资料、签到，以及匿名和公开运维路径拒绝。MySQL、Redis、web 均 healthy；验收时运行容器没有 OOMKilled。

验收按正常登录流程创建了手机号 `18800001234` 对应的普通演示用户并签到一次，没有配置管理员权限。测试不会证明完整领券、秒杀、支付流程或消息中间件链路正常，也没有进行浏览器视觉验收或并发压测。

首次公网检查时，8080 从本地电脑两次连接均超时。用户在阿里云安全组添加并保存入方向 TCP 8080 规则后，2026-10-11 已完成独立公网复测：首页、体验页、优惠券页、本地图片均 HTTP 200；健康状态 UP，分类、店铺列表/详情、缓存观察和功能开关接口成功；匿名本人资料请求 401，公开运维和 Prometheus 路径 404。网站现在可以通过公网地址访问。详细记录见 [云端部署验收](../docs/cloud-deployment-verification.json)。

以下是后续核对清单，已通过的部署检查无需反复重做，业务场景需要另行验证：

- 首页返回真实店铺；图片成功加载。
- 代理分类接口 success=true；Java与数据库未公开宿主端口。
- 正常登录后 `/user/me` 返回当前用户；无登录不能读取本人订单。
- 签到与普通领券正常，重复领取按当前状态返回原订单或明确忙碌。
- 缓存观察有两次读取轨迹。
- Kafka、MQ、CDC显示未启用，避免把开关关闭当故障。
- 专用管理员配置有效，普通访客不能执行管理操作。
- 外部电脑访问成功，内存与容器运行稳定，没有 OOM重启。

服务器部署和公网入口已通过上述验收，可以分享体验链接。领券、秒杀、支付和管理员操作等业务场景仍需按对应章节另行验证；本次部署验收不代表所有业务回归通过。
