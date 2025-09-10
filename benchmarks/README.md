# Redis Proxy 压力测试套件

## 📋 概述

本目录包含了 Redis Proxy 的完整压力测试套件，专门用于测试 Dedicated 模式下的缓存性能和系统稳定性。

## 🧪 测试程序

### 1. 热Key压力测试 (`hotkey_benchmark.go`)

**目的**: 测试缓存对热点数据的性能提升效果

**特性**:
- 模拟真实的热Key访问模式（80%热Key，20%冷Key）
- 可配置的热Key数量和冷Key数量
- 实时统计缓存命中率和延迟差异
- 详细的性能分析报告

**测试指标**:
- 热Key vs 冷Key 延迟对比
- 缓存命中率
- 吞吐量提升
- 延迟分布分析

### 2. 常规查询压力测试 (`regular_query_benchmark.go`)

**目的**: 测试各种读写比例下的系统性能表现

**测试场景**:
- 纯GET测试 (100% 读)
- 纯SET测试 (100% 写)
- 读写混合测试 (80% 读 + 20% 写)
- 重读测试 (95% 读 + 5% 写)
- 重写测试 (30% 读 + 70% 写)

**测试指标**:
- 不同场景下的吞吐量
- 读写操作的延迟差异
- 系统稳定性分析

### 3. GET/SET基础测试 (`getset_benchmark.go`)

**目的**: 测试基础读写操作的性能基线

**特性**:
- 交替执行GET和SET操作
- 基础性能指标收集
- 延迟统计分析

### 4. 连接池压力测试 (`connection_pool_benchmark.go`)

**目的**: 测试连接池管理的效率和稳定性

**特性**:
- 高并发连接测试
- 连接复用效率分析
- 资源管理验证

## 🚀 快速开始

### 前置条件

1. **启动 Redis 服务器**:
   ```bash
   redis-server
   ```

2. **启动 Redis Proxy**:
   ```bash
   ./redis-proxy-demo -c etc/config-dedicated-proxy.yaml
   ```

3. **确保缓存已启用**:
   检查配置文件中 `cache.enabled: true`

### 运行测试

#### 快速测试（推荐）
```bash
cd benchmarks
./quick_test.sh
```

#### 完整测试套件
```bash
cd benchmarks
./run_all_benchmarks.sh
```

#### 单独运行测试
```bash
cd benchmarks

# 热Key测试
go run hotkey_benchmark.go

# 常规查询测试
go run regular_query_benchmark.go

# 基础GET/SET测试
go run getset_benchmark.go

# 连接池测试
go run connection_pool_benchmark.go
```

## 📊 测试配置

### 热Key测试配置
```go
const (
    host           = "127.0.0.1:6380"
    numClients     = 100              // 并发客户端数
    testDuration   = 60 * time.Second // 测试持续时间
    hotKeyRatio    = 0.8              // 热Key访问比例 (80%)
    numHotKeys     = 10               // 热Key数量
    numColdKeys    = 1000             // 冷Key数量
    reportInterval = 5 * time.Second  // 报告间隔
)
```

### 常规查询测试配置
```go
const (
    host           = "127.0.0.1:6380"
    numClients     = 50               // 并发客户端数
    testDuration   = 30 * time.Second // 测试持续时间
    keySpaceSize   = 10000            // Key空间大小
    reportInterval = 5 * time.Second  // 报告间隔
)
```

## 📈 结果分析

### 关键指标

1. **吞吐量 (QPS)**
   - 每秒处理的请求数
   - 不同操作类型的吞吐量对比

2. **延迟分析**
   - 平均延迟、最小延迟、最大延迟
   - 热Key vs 冷Key 延迟对比
   - 延迟分布（P50, P95, P99）

3. **缓存效果**
   - 缓存命中率
   - 性能提升百分比
   - 热Key访问优化效果

4. **稳定性指标**
   - 错误率
   - 连接成功率
   - 超时情况

### 预期结果

#### 缓存启用时
- **热Key延迟**: < 1ms（缓存命中）
- **冷Key延迟**: 1-5ms（Redis查询 + 缓存更新）
- **性能提升**: 热Key访问提升 50-80%
- **吞吐量**: 显著提升，特别是读多写少场景

#### 缓存禁用时
- **所有延迟**: 1-5ms（直接Redis查询）
- **性能**: 稳定但无缓存加速

## 🔧 自定义测试

### 修改测试参数

1. **调整并发数**:
   ```go
   numClients = 200  // 增加并发客户端
   ```

2. **修改测试时长**:
   ```go
   testDuration = 120 * time.Second  // 延长测试时间
   ```

3. **调整热Key比例**:
   ```go
   hotKeyRatio = 0.9  // 90%热Key访问
   ```

4. **修改Key空间**:
   ```go
   numHotKeys = 20    // 增加热Key数量
   numColdKeys = 5000 // 增加冷Key数量
   ```

### 添加新的测试场景

1. 复制现有测试文件
2. 修改测试逻辑和参数
3. 更新测试脚本

## 📋 故障排查

### 常见问题

1. **连接失败**
   ```
   ❌ Redis Proxy 未运行
   ```
   **解决**: 确保代理服务器已启动

2. **编译错误**
   ```
   ❌ 编译失败
   ```
   **解决**: 检查Go环境和依赖

3. **高错误率**
   - 检查系统资源使用情况
   - 降低并发数或测试强度
   - 检查网络连接稳定性

4. **性能异常**
   - 确认缓存配置正确
   - 检查Redis服务器性能
   - 监控系统资源使用

### 调试技巧

1. **启用详细日志**:
   ```yaml
   cache:
     verbose: true
   ```

2. **监控系统资源**:
   ```bash
   top -p $(pgrep redis-proxy-demo)
   ```

3. **检查连接状态**:
   ```bash
   netstat -an | grep 6380
   ```

## 📊 基准数据

### 测试环境
- **CPU**: Apple M1 Pro
- **内存**: 16GB
- **Redis**: 7.0+
- **Go**: 1.21+

### 参考性能（缓存启用）
- **热Key延迟**: ~0.5ms
- **冷Key延迟**: ~2ms
- **吞吐量**: 50,000+ QPS
- **缓存命中率**: 80%+
- **性能提升**: 60%+

## 🔄 持续优化

### 性能调优建议

1. **缓存配置优化**
   - 根据内存情况调整 `hard_max_cache_size`
   - 优化TTL设置
   - 调整清理间隔

2. **连接池优化**
   - 调整最大连接数
   - 优化连接超时设置
   - 监控连接使用情况

3. **系统级优化**
   - 调整操作系统网络参数
   - 优化内存分配
   - 监控GC性能

### 监控建议

1. 定期运行基准测试
2. 监控关键性能指标
3. 建立性能基线
4. 设置告警阈值

---

**更新时间**: 2025-09-10  
**版本**: v1.0.0
