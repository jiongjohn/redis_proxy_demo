# Go-Redis 客户端连接 Redis 代理演示

这个演示程序展示了如何使用 `github.com/redis/go-redis/v9` 客户端连接到我们的智能连接池 Redis 代理。

## 🎯 功能演示

### 1. 基础操作
- SET/GET/EXISTS/DEL 基本命令
- 验证基础缓存操作功能

### 2. 大Key处理
- **10KB 数据存储和读取**  
- **验证数据完整性** (修复的大key bug)
- 确保大数据无截断传输

### 3. 数据类型操作
- **List**: LPUSH, LLEN, LRANGE
- **Hash**: HSET, HLEN, HGETALL  
- **Set**: SADD, SCARD, SMEMBERS

### 4. 高级特性
- **事务操作**: MULTI/EXEC 批量命令
- **管道操作**: Pipeline 批量处理
- **连接信息**: PING, INFO, 连接池统计

### 5. 性能测试
- SET/GET 操作吞吐量测试
- 连接复用验证

## 🚀 运行演示

### 前置条件

1. **启动 Redis 服务器**:
   ```bash
   # 确保Redis运行在localhost:6379
   redis-server
   ```

2. **启动智能连接池代理**:
   ```bash
   cd ../../  # 回到项目根目录
   ./redis-proxy-demo -c etc/config-intelligent-pool.yaml
   ```

### 运行演示程序

```bash
# 进入演示目录
cd example/go-redis-client-demo

# 初始化Go模块（首次运行）
go mod tidy

# 运行演示
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
