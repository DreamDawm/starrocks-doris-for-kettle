# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

StarRocks Kettle Connector 是一个 Pentaho Kettle (PDI) 插件，用于将数据从各种数据源导入到 StarRocks 或 Apache Doris 数据库。插件通过 Stream Load 协议实现高效的数据写入，支持 StarRocks 和 Doris 两种数据库类型。

## Build Commands

```bash
# 构建插件包（跳过测试和许可证检查）
mvn clean package -Dmaven.test.skip=true -Drat.skip=true

# 构建插件包（运行测试，需要提供数据库连接参数）
mvn clean package \
  -Dhttp_urls=<fe_http_url> \
  -Djdbc_urls=<fe_jdbc_url> \
  -Duser=<username> \
  -Dpassword=<password>
```

构建产物位于 `assemblies/plugin/target/starrocks-kettle-connector-plugins-1.0-SNAPSHOT-plugins.zip`

## Architecture

```
starrocks-connector-for-kettle/
├── assemblies/plugin/          # 打包模块，生成插件 ZIP
│   └── src/main/assembly/      # Assembly 配置
├── impl/                       # 核心实现模块
│   └── src/main/java/.../
│       └── steps/starrockskettleconnector/
│           ├── StarRocksKettleConnector.java       # Step 执行器（继承 BaseStep）
│           ├── StarRocksKettleConnectorData.java   # Step 数据类
│           ├── StarRocksKettleConnectorMeta.java   # Step 元数据（配置参数）
│           ├── core/                               # 核心抽象层
│           │   ├── DatabaseType.java               # 数据库类型枚举（STARROCKS/DORIS）
│           │   ├── StreamLoadClient.java           # Stream Load 客户端接口
│           │   ├── StreamLoadClientFactory.java    # 客户端工厂
│           │   ├── StreamLoadConfig.java           # 统一配置类
│           │   ├── Serializer.java                 # 序列化接口
│           │   └── DataType.java                   # 数据类型枚举
│           ├── doris/                              # Doris 实现
│           │   ├── DorisStreamLoadClient.java      # Doris Stream Load 客户端
│           │   └── DorisQueryVisitor.java         # Doris 表元数据查询
│           └── starrocks/                          # StarRocks 实现
│               ├── StarRocksStreamLoadClient.java   # StarRocks Stream Load 客户端
│               ├── StarRocksCsvSerializer.java      # CSV 序列化
│               ├── StarRocksJsonSerializer.java     # JSON 序列化
│               ├── StarRocksJdbcConnectionProvider.java  # JDBC 连接管理
│               └── StarRocksQueryVisitor.java       # 表元数据查询
└── ui/                         # UI 模块
    └── src/main/java/.../
        └── StarRocksKettleConnectorDialog.java       # Kettle UI 对话框
```

### 核心类说明

- **StarRocksKettleConnector**: 继承 `BaseStep`，实现 `processRow()` 处理每一行数据
- **StarRocksKettleConnectorMeta**: 使用 `@Injection` 注解定义配置参数，支持 Kettle 的依赖注入机制
- **StreamLoadClientFactory**: 根据数据库类型创建对应的 Stream Load 客户端
- **DorisStreamLoadClient**: Doris 专用客户端，使用 HTTP PUT 实现 Stream Load，支持 307 重定向
- **StarRocksStreamLoadClient**: StarRocks 客户端，封装 StreamLoadManagerV2 SDK
- **StarRocksCsvSerializer**: CSV 序列化器，自动去除换行符、回车符、制表符

### Kettle 插件规范

遵循 Kettle 插件规范：
- 使用 `@Step` 注解声明 Step 类型
- 使用 `@InjectionSupported` 声明可注入属性
- `Meta` 类负责配置序列化和 UI 绑定
- `Data` 类负责运行时状态管理
- `Dialog` 类负责 UI 交互

## UI 配置字段

插件 UI 支持以下配置字段：
- 步骤名称 (Step Name)
- Http URL (FE HTTP 地址)
- JDBC URL (FE JDBC 地址)
- 数据库类型 (Database Type: StarRocks/Doris)
- 数据库名称 (Database Name)
- 表名称 (Table Name)
- 用户名 (Username)
- 密码 (Password)
- 数据格式 (Format: CSV/JSON)
- 列分隔符 (Column Separator)

## 测试

测试位于 `impl/src/test/java/`，运行测试需要 StarRocks/Doris 实例：
- `StarRocksKettleConnectorMetaTest` - 元数据测试
- `StarRocksKettleConnectorTest` - 连接器测试
- `StarRocksKettleConnectorWriteTest` - 写入测试
- `StarRocksKettleConnectorPartialUpdateTest` - 部分更新测试

## 技术栈

- **Kettle/PDI**: 9.4.0.0-343
- **StarRocks Stream Load SDK**: 1.0-SNAPSHOT
- **Java**: JDK 11
- **Maven**: 3+
- **Apache HttpClient**: 4.5.13 (用于 Doris Stream Load)

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **starrocks-connector-for-kettle** (1206 symbols, 2151 relationships, 35 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/starrocks-connector-for-kettle/context` | Codebase overview, check index freshness |
| `gitnexus://repo/starrocks-connector-for-kettle/clusters` | All functional areas |
| `gitnexus://repo/starrocks-connector-for-kettle/processes` | All execution flows |
| `gitnexus://repo/starrocks-connector-for-kettle/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |
| Work in the Starrockskettleconnector area (107 symbols) | `.claude/skills/generated/starrockskettleconnector/SKILL.md` |
| Work in the Starrocks area (16 symbols) | `.claude/skills/generated/starrocks/SKILL.md` |

<!-- gitnexus:end -->
