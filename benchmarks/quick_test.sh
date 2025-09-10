#!/bin/bash

# Redis Proxy 快速测试脚本

echo "⚡ Redis Proxy 快速压力测试"
echo "=========================="

# 检查代理服务器是否运行
echo "📋 检查代理服务器状态..."
if ! nc -z localhost 6380 > /dev/null 2>&1; then
    echo "❌ Redis Proxy 未运行，请先启动代理服务器"
    echo "   启动命令: ./redis-proxy-demo -c etc/config-dedicated-proxy.yaml"
    exit 1
fi
echo "✅ Redis Proxy 正常运行"

# 编译热Key测试
echo ""
echo "🔧 编译热Key测试程序..."
cd benchmarks
go build -o hotkey_benchmark hotkey_benchmark.go

if [ $? -ne 0 ]; then
    echo "❌ 编译失败"
    exit 1
fi
echo "✅ 编译完成"

# 运行快速热Key测试（缩短测试时间）
echo ""
echo "🔥 运行快速热Key测试（30秒）..."
echo "   这将测试缓存对热点数据的性能提升效果"
echo ""

# 修改测试参数为快速测试
sed -i.bak 's/testDuration   = 60 \* time.Second/testDuration   = 30 * time.Second/' hotkey_benchmark.go
sed -i.bak 's/numClients     = 100/numClients     = 50/' hotkey_benchmark.go

# 重新编译
go build -o hotkey_benchmark hotkey_benchmark.go

# 运行测试
./hotkey_benchmark

# 恢复原始文件
mv hotkey_benchmark.go.bak hotkey_benchmark.go

# 清理
rm -f hotkey_benchmark

echo ""
echo "✅ 快速测试完成！"
echo ""
echo "🔍 结果分析："
echo "1. 观察热Key和冷Key的延迟差异"
echo "2. 检查缓存命中对性能的提升效果"
echo "3. 注意错误率和连接稳定性"
echo ""
echo "💡 如需完整测试，请运行: ./run_all_benchmarks.sh"
