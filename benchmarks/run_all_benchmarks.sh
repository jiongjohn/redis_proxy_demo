#!/bin/bash

# Redis Proxy 综合压力测试脚本

echo "🚀 Redis Proxy 综合压力测试"
echo "============================"

# 检查 Redis 是否运行
echo "📋 检查 Redis 服务状态..."
if ! redis-cli ping > /dev/null 2>&1; then
    echo "❌ Redis 服务未运行，请先启动 Redis"
    echo "   启动命令: redis-server"
    exit 1
fi
echo "✅ Redis 服务正常运行"

# 检查代理服务器是否运行
echo "📋 检查代理服务器状态..."
if ! nc -z localhost 6380 > /dev/null 2>&1; then
    echo "❌ Redis Proxy 未运行，请先启动代理服务器"
    echo "   启动命令: ./redis-proxy-demo -c etc/config-dedicated-proxy.yaml"
    exit 1
fi
echo "✅ Redis Proxy 正常运行"

# 创建结果目录
RESULT_DIR="benchmark_results_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULT_DIR"
echo "📁 结果将保存到: $RESULT_DIR"

# 编译测试程序
echo ""
echo "🔧 编译测试程序..."
go build -o hotkey_benchmark hotkey_benchmark.go
go build -o regular_query_benchmark regular_query_benchmark.go
go build -o getset_benchmark getset_benchmark.go
go build -o connection_pool_benchmark connection_pool_benchmark.go

if [ $? -ne 0 ]; then
    echo "❌ 编译失败"
    exit 1
fi
echo "✅ 编译完成"

# 测试函数
run_test() {
    local test_name="$1"
    local test_cmd="$2"
    local output_file="$3"
    
    echo ""
    echo "🧪 运行测试: $test_name"
    echo "   命令: $test_cmd"
    echo "   输出: $output_file"
    echo "   开始时间: $(date)"
    
    # 运行测试并保存输出
    eval "$test_cmd" | tee "$RESULT_DIR/$output_file"
    
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        echo "✅ $test_name 完成"
    else
        echo "❌ $test_name 失败"
    fi
    
    echo "   结束时间: $(date)"
}

# 系统信息收集
echo ""
echo "📊 收集系统信息..."
{
    echo "系统信息收集时间: $(date)"
    echo "================================"
    echo "操作系统: $(uname -a)"
    echo "CPU信息:"
    sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "无法获取CPU信息"
    echo "内存信息:"
    system_profiler SPHardwareDataType 2>/dev/null | grep "Memory:" || echo "无法获取内存信息"
    echo "Go版本: $(go version)"
    echo "Redis版本:"
    redis-cli --version 2>/dev/null || echo "无法获取Redis版本"
    echo ""
    echo "代理服务器配置:"
    echo "- 监听端口: 6380"
    echo "- 目标Redis: localhost:6379"
    echo "- 缓存状态: 已启用"
    echo ""
} > "$RESULT_DIR/system_info.txt"

# 运行测试套件
echo ""
echo "🎯 开始运行测试套件..."

# 1. 热Key压力测试
run_test "热Key压力测试" "./hotkey_benchmark" "hotkey_benchmark.log"

# 等待间隔
echo "⏳ 等待10秒后继续..."
sleep 10

# 2. 常规查询压力测试
run_test "常规查询压力测试" "./regular_query_benchmark" "regular_query_benchmark.log"

# 等待间隔
echo "⏳ 等待10秒后继续..."
sleep 10

# 3. GET/SET基础测试
run_test "GET/SET基础测试" "./getset_benchmark" "getset_benchmark.log"

# 等待间隔
echo "⏳ 等待10秒后继续..."
sleep 10

# 4. 连接池测试
run_test "连接池压力测试" "./connection_pool_benchmark" "connection_pool_benchmark.log"

# 生成综合报告
echo ""
echo "📋 生成综合报告..."
{
    echo "Redis Proxy 综合压力测试报告"
    echo "============================="
    echo "测试时间: $(date)"
    echo "测试目录: $RESULT_DIR"
    echo ""
    echo "测试概述:"
    echo "1. 热Key压力测试 - 测试缓存对热点数据的性能提升"
    echo "2. 常规查询压力测试 - 测试各种读写比例下的性能表现"
    echo "3. GET/SET基础测试 - 测试基础读写操作性能"
    echo "4. 连接池压力测试 - 测试连接池管理效率"
    echo ""
    echo "详细结果请查看各个测试的日志文件:"
    ls -la "$RESULT_DIR"/*.log 2>/dev/null | while read line; do
        echo "- $line"
    done
    echo ""
    echo "系统信息详见: system_info.txt"
    echo ""
} > "$RESULT_DIR/summary_report.txt"

# 清理编译文件
echo ""
echo "🧹 清理编译文件..."
rm -f hotkey_benchmark regular_query_benchmark getset_benchmark connection_pool_benchmark

# 完成
echo ""
echo "🎉 所有测试完成！"
echo "📊 测试结果保存在: $RESULT_DIR/"
echo "📋 查看综合报告: cat $RESULT_DIR/summary_report.txt"
echo ""
echo "主要测试文件:"
echo "├── hotkey_benchmark.log - 热Key测试结果"
echo "├── regular_query_benchmark.log - 常规查询测试结果"
echo "├── getset_benchmark.log - GET/SET基础测试结果"
echo "├── connection_pool_benchmark.log - 连接池测试结果"
echo "├── system_info.txt - 系统信息"
echo "└── summary_report.txt - 综合报告"
echo ""
echo "🔍 分析建议:"
echo "1. 对比热Key和冷Key的延迟差异，评估缓存效果"
echo "2. 观察不同读写比例下的性能表现"
echo "3. 检查错误率和连接稳定性"
echo "4. 分析延迟分布，识别性能瓶颈"
