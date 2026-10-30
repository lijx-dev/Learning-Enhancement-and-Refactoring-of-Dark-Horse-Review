# 开发与提交约定

先阅读 [项目说明](README.md) 和 [Wiki](wiki/README.md)。本文档面向源码协作；完整中间件回归使用独立测试环境。

## 本地检查

```bash
python scripts/check-repository.py
npm --prefix frontend run check
npm --prefix frontend test
mvn -B -Dtest=MapperArchitectureTest,CacheReadTraceTest,RequestCacheScopeTest,CanalBatchTest,CacheChangeProjectorTest,SeckillMessageTest,ExperienceServiceTest package
```

Java 使用 JDK 8、Maven 3.9；前端测试使用 Node.js 18+。Maven 会执行 Checkstyle。上述选择的测试不启动业务数据库；完整集成回归见 [测试教程](wiki/13-测试监控与故障排查.md)，不要将测试指向已有业务数据。

## 修改原则

- 保持 Controller → Service → Mapper 分层，数据库结构用新的 Flyway 迁移版本演进。
- 同时修改对应 Wiki、接口或配置说明；性能数字必须附场景、数据量和测量边界。
- `.env`、凭证、用户上传、`target/`、IDE 状态和原始日志不提交。示例配置只使用明确的本地演示值。
- 提交前执行 `git diff --check`，检查 `git status --short` 与暂存区文件。
- 不随意删除已有文档、课程来源说明或第三方版权注释。

## 来源与许可边界

项目基于黑马点评课程及原有学习代码继续改造，本地 Git 中保留原有提交历史。已配置的上游地址为 [Biscuit-AI531/hm-dianping](https://github.com/Biscuit-AI531/hm-dianping)。这个地址用于说明已有代码来源，不代表已独立核实每个文件的著作权。

课程 SQL、素材和 `frontend/vendor/vue.js` 等第三方文件仍受各自权利和许可约束。当前仓库没有统一添加 MIT/Apache 许可；进一步分发前应分别确认来源。Vue 文件头的 MIT 许可声明保留。
