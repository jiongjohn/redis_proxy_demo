# Go-Redis 客户端连接 Redis 代理演示

这个演示程序展示了如何使用 `github.com/redis/go-redis/v9` 客户端连接到我们的专用连接池 Redis 代理，包含多种测试脚本。

## 🎯 测试脚本说明

### 1. 核心命令测试 (`core_commands_test.go`)
专门测试最常用的 Redis 命令：
- **字符串操作**: SET, GET, SETNX, GETSET, INCR, DECR, APPEND
- **哈希操作**: HSET, HGET, HMSET, HMGET, HGETALL, HDEL, HEXISTS  
- **集合操作**: SADD, SMEMBERS, SCARD, SISMEMBER, SPOP, SREM
- **列表操作**: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN
- **原子操作**: INCR, DECR, INCRBY, DECRBY, SETNX
- **过期操作**: EXPIRE, TTL, SETEX, PERSIST

### 2. 全面命令测试 (`comprehensive_test.go`)
包含所有 Redis 数据类型和高级功能：
- **字符串、哈希、列表、集合、有序集合**
- **位图操作**: SETBIT, GETBIT, BITCOUNT
- **事务操作**: MULTI/EXEC
- **管道操作**: Pipeline 批量处理
- **大数据测试**: 1KB, 10KB, 100KB 数据
- **并发测试**: 多goroutine并发操作
- **性能基准测试**

### 3. 简单测试 (`simple_demo.go`)
基础连接和操作验证：
- 连接测试
- 基本 SET/GET 操作
- 中等大小数据测试

### 4. 原有完整测试 (`main.go`)
原始的综合演示程序：
- 基础操作、大key处理、数据类型操作
- 事务、管道、连接信息、性能测试

## 🚀 运行演示

### 前置条件

1. **启动 Redis 服务器**:
   ```bash
   # 确保Redis运行在localhost:6379
   redis-server
   ```

2. **启动专用连接池代理**:
   ```bash
   cd ../../  # 回到项目根目录
   ./redis-proxy-demo -c etc/config-dedicated-proxy.yaml
   ```

### 快速运行（推荐）

```bash
# 进入演示目录
cd example/go-redis-client-demo

# 初始化Go模块（首次运行）
go mod tidy

# 使用交互式脚本运行测试
./run_tests.sh
```

### 手动运行各个测试

```bash
# 1. 核心命令测试（推荐开始）
go run core_commands.go

# 2. 全面命令测试
go run comprehensive.go

# 3. 简单测试
go run simple_demo.go

# 4. 原有完整测试
go run main.go
```

## 📊 预期输出

```
🚀 Go-Redis客户端连接Redis代理演示
==================================================

📝 1. 基础 SET/GET 操作测试
✅ SET demo:hello world
✅ GET demo:hello → world
✅ EXISTS demo:hello → 1
✅ DEL demo:hello → 1

📦 2. 大Key操作测试 (验证数据完整性)
✅ SET demo:bigkey (大小: 10240 字节)
✅ GET demo:bigkey (大小: 10240 字节) - 数据完整性验证通过

🎯 3. 各种数据类型操作测试
✅ List长度: 3
✅ List内容: [item3 item2 item1]
✅ Hash字段数: 2
✅ Hash内容: map[field1:value1 field2:value2]
✅ Set成员数: 3
✅ Set成员: [member1 member2 member3]

🔄 4. 事务操作测试 (MULTI/EXEC)
✅ 事务执行成功，4个命令
✅ 计数器值: 2

⚡ 5. 管道操作测试
✅ 管道批量操作成功，5个命令
✅ Pipe GET key0 → value0
✅ Pipe GET key1 → value1
...

🔗 6. 连接信息测试
✅ PING → PONG
✅ Redis服务器信息:
   # Server
   redis_version:6.2.6
   redis_git_sha1:00000000
✅ 连接池统计:
   总连接数: 1
   空闲连接数: 1
   过期连接数: 0

🏆 7. 性能测试
✅ 性能测试结果 (100次操作):
   SET: 45.2ms (2212.39 ops/sec)
   GET: 38.7ms (2583.98 ops/sec)

✅ 所有测试完成！
```

## 🔧 配置说明

### Go-Redis 客户端配置

```go
rdb := redis.NewClient(&redis.Options{
    Addr:     "localhost:6380", // 连接代理端口，非Redis端口
    Password: "",               // 无密码
    DB:       0,                // 默认数据库
    
    // 连接池配置
    PoolSize:        10,        // 连接池大小
    MinIdleConns:    2,         // 最小空闲连接数  
    MaxIdleConns:    5,         // 最大空闲连接数
    ConnMaxIdleTime: 30 * time.Second,
    ConnMaxLifetime: 10 * time.Minute,
})
```

### 关键配置要点

- **Addr**: 必须指向代理端口 `6380`，而不是 Redis 端口 `6379`
- **连接池**: Go-Redis 客户端自己的连接池 + 代理的智能连接池 = 双层优化
- **超时设置**: 客户端超时应该大于代理的命令超时设置

## 🛠 故障排除

### 连接失败
```
dial tcp :6380: connect: connection refused
```
**解决**: 确保智能连接池代理正在运行

### 大key数据不完整  
```
❌ 大key数据不完整! 期望: 10240, 实际: 4096
```
**解决**: 这表明代理的大key修复可能有问题，检查代理日志

### 性能异常低
```
SET: 2.5s (40 ops/sec)
```
**解决**: 检查网络延迟和代理配置，正常应该在 1000+ ops/sec

## 🎯 验证要点

1. **数据完整性**: 大key数据长度必须完全匹配
2. **连接复用**: 连接池统计显示连接被复用  
3. **事务支持**: MULTI/EXEC 命令正确执行
4. **性能表现**: 通过代理的性能应接近直连

## 📝 代码结构

```
example/go-redis-client-demo/
├── main.go       # 主演示程序
├── go.mod        # Go模块依赖  
└── README.md     # 本文档
```

这个演示全面验证了 Redis 代理的功能正确性和性能表现！🚀
