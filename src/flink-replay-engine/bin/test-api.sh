#!/bin/bash

# API 测试脚本
# =============

BASE_URL="http://localhost:8080/api/v1/replay"

echo "========================================"
echo "  回放引擎 API 测试脚本"
echo "========================================"
echo ""

# 健康检查
echo "[1/5] 健康检查..."
curl -s "$BASE_URL/health" | python3 -m json.tool 2>/dev/null || curl -s "$BASE_URL/health"
echo ""

# 启动第一个回放任务 (基线)
echo "[2/5] 启动基线回放任务..."
BASELINE_JOB=$(curl -s -X POST "$BASE_URL/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "jobId": "baseline-demo",
    "databaseName": "test-db",
    "tableName": "test-table",
    "region": "eu-central-1",
    "startTime": 1700000000000,
    "endTime": 1700003600000,
    "speedFactor": 10.0,
    "enableCheckpoint": false,
    "enableDirtyDataIsolation": true,
    "dirtyDataThreshold": 3.0,
    "rcfParams": {
      "shingleSize": 1,
      "numberOfTrees": 50,
      "sampleSize": 8192
    }
  }')

echo "$BASELINE_JOB" | python3 -m json.tool 2>/dev/null || echo "$BASELINE_JOB"
echo ""

# 启动第二个回放任务 (测试)
echo "[3/5] 启动测试回放任务..."
TEST_JOB=$(curl -s -X POST "$BASE_URL/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "jobId": "test-demo",
    "databaseName": "test-db",
    "tableName": "test-table",
    "region": "eu-central-1",
    "startTime": 1700000000000,
    "endTime": 1700003600000,
    "speedFactor": 10.0,
    "enableCheckpoint": false,
    "enableDirtyDataIsolation": true,
    "dirtyDataThreshold": 2.5,
    "rcfParams": {
      "shingleSize": 2,
      "numberOfTrees": 100,
      "sampleSize": 4096
    }
  }')

echo "$TEST_JOB" | python3 -m json.tool 2>/dev/null || echo "$TEST_JOB"
echo ""

# 等待任务完成
echo "[4/5] 等待任务完成..."
sleep 5

echo "  基线任务状态:"
curl -s "$BASE_URL/jobs/baseline-demo" | python3 -m json.tool 2>/dev/null | head -10 || curl -s "$BASE_URL/jobs/baseline-demo"
echo ""

echo "  测试任务状态:"
curl -s "$BASE_URL/jobs/test-demo" | python3 -m json.tool 2>/dev/null | head -10 || curl -s "$BASE_URL/jobs/test-demo"
echo ""

# 生成差异报告
echo "[5/5] 生成差异报告..."
DIFF_REPORT=$(curl -s -X POST "$BASE_URL/diff" \
  -H "Content-Type: application/json" \
  -d '{
    "jobId": "test-demo",
    "baselineJobId": "baseline-demo"
  }')

echo "$DIFF_REPORT" | python3 -m json.tool 2>/dev/null || echo "$DIFF_REPORT"
echo ""

echo "========================================"
echo "  测试完成!"
echo "========================================"
