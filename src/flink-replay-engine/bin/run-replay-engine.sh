#!/bin/bash

# 异常特征回放引擎 - 完整启动脚本
# ====================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR/.."

echo "========================================"
echo "  异常特征回放引擎"
echo "========================================"
echo ""

# 构建项目
echo "[1/3] 构建项目..."
cd "$PROJECT_DIR"
mvn clean package -DskipTests -q
echo "  构建完成!"
echo ""

# 查找 JAR 包
JAR_FILE=$(find target -name "*.jar" ! -name "*sources*" ! -name "*javadoc*" ! -name "*original*" | head -1)

if [ -z "$JAR_FILE" ]; then
    echo "[ERROR] 未找到构建产物 JAR 文件"
    exit 1
fi

echo "[2/3] 运行本地演示..."
echo ""

# Java 21+ 需要的 JVM 参数
JVM_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED"
JVM_OPTS="$JVM_OPTS --add-opens java.base/java.util=ALL-UNNAMED"
JVM_OPTS="$JVM_OPTS --add-opens java.base/java.util.concurrent=ALL-UNNAMED"
JVM_OPTS="$JVM_OPTS --add-opens java.base/java.io=ALL-UNNAMED"
JVM_OPTS="$JVM_OPTS --add-opens java.base/java.lang.reflect=ALL-UNNAMED"
JVM_OPTS="$JVM_OPTS --add-opens java.base/java.math=ALL-UNNAMED"
JVM_OPTS="$JVM_OPTS --add-opens java.base/java.net=ALL-UNNAMED"
JVM_OPTS="$JVM_OPTS --add-opens java.base/java.nio=ALL-UNNAMED"
JVM_OPTS="$JVM_OPTS --add-opens java.base/sun.nio.ch=ALL-UNNAMED"
JVM_OPTS="$JVM_OPTS --add-opens java.base/sun.nio.cs=ALL-UNNAMED"
JVM_OPTS="$JVM_OPTS --add-opens java.base/sun.security.action=ALL-UNNAMED"
JVM_OPTS="$JVM_OPTS --add-opens java.base/sun.util.calendar=ALL-UNNAMED"

java $JVM_OPTS -cp "$JAR_FILE" com.amazonaws.services.replay.demo.LocalReplayDemo

echo ""
echo "[3/3] 启动 REST API 服务..."
echo "  服务地址: http://localhost:8080/api/v1/replay"
echo "  健康检查: http://localhost:8080/api/v1/replay/health"
echo ""
echo "  按 Ctrl+C 停止服务"
echo ""

# 启动 REST API 服务
java $JVM_OPTS -cp "$JAR_FILE" com.amazonaws.services.replay.ReplayEngineApplication 8080
