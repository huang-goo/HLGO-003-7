#!/bin/bash

# 异常特征回放引擎 - 快速演示脚本
# ====================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR/.."

echo "========================================"
echo "  异常特征回放引擎 - 快速演示"
echo "========================================"
echo ""

# 构建项目
echo "[1/2] 构建项目..."
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

echo "[2/2] 运行本地演示..."
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

java $JVM_OPTS -jar "$JAR_FILE" --demo

echo ""
echo "演示完成!"
