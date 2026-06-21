# 异常特征回放引擎 (Anomaly Feature Replay Engine)

## 项目概述

基于 Flink MiniCluster 的异常特征回放引擎，支持从 Timestream 按时间区间拉取历史点位数据，重放至本地 Flink 环境以验证 RCF (Random Cut Forest) 模型参数效果。

## 核心功能

### 1. 数据拉取 (Timestream Source)
- 按时间区间从 AWS Timestream 拉取点位数据
- 支持分页查询和指标过滤
- 自动解析时间戳和维度信息

### 2. 回放引擎 (Flink MiniCluster)
- 本地 Flink MiniCluster 执行环境
- RCF 随机切分森林异常检测算法
- 支持自定义模型参数调优

### 3. 快照恢复 (Snapshot & Recovery)
- Checkpoint 周期性快照
- Savepoint 手动保存点
- 支持从快照恢复作业状态

### 4. 倍速控制 (Speed Control)
- 支持自定义回放速度 (0.1x ~ 100x)
- 事件时间驱动的回放节奏
- 精确控制数据发射速率

### 5. 脏数据隔离 (Dirty Data Isolation)
- Z-score 异常值检测
- 格式校验和空值过滤
- 旁路输出脏数据记录
- 可配置的异常阈值

### 6. 差异报告 (Diff Report)
- 两次回放结果对比分析
- 异常得分差异统计
- 新增/漏检异常点识别
- 自动生成洞察结论

### 7. REST API 控制
- 完整的 RESTful API 接口
- 作业生命周期管理
- 结果查询与报告生成

## 项目结构

```
flink-replay-engine/
├── pom.xml
├── bin/
│   ├── run-replay-engine.sh    # 完整启动脚本
│   ├── quick-demo.sh           # 快速演示脚本
│   └── test-api.sh             # API 测试脚本
└── src/main/
    ├── java/com/amazonaws/services/replay/
    │   ├── ReplayEngineApplication.java    # 启动入口
    │   ├── api/
    │   │   └── ReplayResource.java         # REST API
    │   ├── core/
    │   │   ├── ReplayEngine.java           # 回放引擎核心
    │   │   ├── SnapshotManager.java        # 快照管理
    │   │   └── DiffReportGenerator.java    # 差异报告生成
    │   ├── source/
    │   │   ├── TimestreamDataFetcher.java  # Timestream 数据拉取
    │   │   └── ReplaySourceFunction.java   # 回放源函数 (带倍速)
    │   ├── operator/
    │   │   ├── DirtyDataFilter.java        # 脏数据过滤
    │   │   ├── MeasureGroupAssigner.java   # 指标分组
    │   │   └── RcfAnomalyDetector.java     # RCF 异常检测
    │   ├── sink/
    │   │   └── ResultCollectSink.java      # 结果收集 Sink
    │   ├── model/
    │   │   ├── ReplayPoint.java            # 点位数据模型
    │   │   ├── ReplayJobRequest.java       # 作业请求
    │   │   ├── ReplayJobResult.java        # 作业结果
    │   │   └── DiffReport.java             # 差异报告
    │   ├── util/
    │   │   └── MockDataGenerator.java      # 模拟数据生成
    │   └── demo/
    │       └── LocalReplayDemo.java        # 本地演示程序
    └── resources/
        ├── log4j2.properties               # 日志配置
        └── replay.properties               # 应用配置
```

## 快速开始

### 前置要求
- Java 11+
- Maven 3.6+
- (可选) AWS 凭证 (访问真实 Timestream)

### 快速演示 (无需 AWS)

```bash
cd src/flink-replay-engine
chmod +x bin/*.sh
./bin/quick-demo.sh
```

### 完整启动 (REST API 服务)

```bash
cd src/flink-replay-engine
chmod +x bin/*.sh
./bin/run-replay-engine.sh
```

服务启动后访问:
- 健康检查: `http://localhost:8080/api/v1/replay/health`
- API 文档: (见下)

## API 接口

### 1. 启动回放任务
```http
POST /api/v1/replay/jobs
Content-Type: application/json

{
  "jobId": "test-job-001",
  "databaseName": "kdaflink",
  "tableName": "kinesisdata1",
  "region": "eu-central-1",
  "startTime": 1700000000000,
  "endTime": 1700003600000,
  "speedFactor": 2.0,
  "enableCheckpoint": true,
  "checkpointInterval": 60000,
  "enableDirtyDataIsolation": true,
  "dirtyDataThreshold": 3.0,
  "measureNames": ["xmeas_1", "xmeas_2"],
  "rcfParams": {
    "shingleSize": 1,
    "shingleCyclic": false,
    "numberOfTrees": 50,
    "sampleSize": 8192,
    "lambda": 0.00001220703125,
    "randomSeed": 42
  }
}
```

### 2. 查询任务状态
```http
GET /api/v1/replay/jobs/{jobId}
```

### 3. 查询异常检测结果
```http
GET /api/v1/replay/jobs/{jobId}/anomalies?limit=100&offset=0
```

### 4. 查询脏数据
```http
GET /api/v1/replay/jobs/{jobId}/dirty?limit=100&offset=0
```

### 5. 停止任务
```http
POST /api/v1/replay/jobs/{jobId}/stop
```

### 6. 清理任务
```http
DELETE /api/v1/replay/jobs/{jobId}
```

### 7. 生成差异报告
```http
POST /api/v1/replay/diff
Content-Type: application/json

{
  "jobId": "test-job",
  "baselineJobId": "baseline-job"
}
```

### 8. 健康检查
```http
GET /api/v1/replay/health
```

## 核心参数说明

### RCF 模型参数
| 参数 | 默认值 | 说明 |
|------|--------|------|
| shingleSize | 1 | 时间窗口大小，增加可捕获时序模式 |
| numberOfTrees | 50 | 森林中树的数量，越多越准确但越慢 |
| sampleSize | 8192 | 每棵树的样本容量 |
| lambda | 0.000012207 | 衰减因子，控制旧数据权重 |
| randomSeed | 42 | 随机种子，保证可复现性 |

### 回放参数
| 参数 | 默认值 | 说明 |
|------|--------|------|
| speedFactor | 1.0 | 回放倍速，2.0 表示 2 倍速 |
| dirtyDataThreshold | 3.0 | Z-score 异常阈值 |
| enableCheckpoint | true | 是否启用 Checkpoint |
| checkpointInterval | 60000 | Checkpoint 间隔 (ms) |

## 差异报告字段说明

| 字段 | 说明 |
|------|------|
| avgScoreDiff | 平均异常得分差异 |
| maxScoreDiff | 最大异常得分差异 |
| anomalyCountDiff | 异常点数量差异 |
| precisionDiff | 精确率差异 |
| recallDiff | 召回率差异 |
| diffType | 差异类型: BOTH_ANOMALY / NEW_ANOMALY / MISSED_ANOMALY / SCORE_DIFF |

## 使用示例

### 验证模型参数效果

1. 运行基线任务 (默认参数)
2. 调整参数后运行测试任务
3. 生成差异报告对比效果
4. 根据洞察结论迭代优化参数

### 典型调参场景

- **提高敏感度**: 增加 numberOfTrees, 减小 sampleSize
- **降低误报**: 增加 sampleSize, 增大 shingleSize
- **关注近期模式**: 增大 lambda 值

## 注意事项

1. 本项目使用本地 Flink MiniCluster，适合开发测试和小规模验证
2. 生产环境建议部署到正式 Flink 集群
3. 访问真实 Timestream 需要配置 AWS 凭证
4. 数据量较大时建议降低回放倍速以保证处理性能
