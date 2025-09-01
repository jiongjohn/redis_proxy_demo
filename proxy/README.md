# 🚀 Redis Proxy 

Redis 代理服务器实现，提供高性能的命令转发功能。

## ✨ 功能特点

- ⚡ **高性能**: 基于 Go 协程的并发处理
- 🔄 **命令转发**: 透明转发 Redis 命令和响应  
- 🏊‍♂️ **连接池**: 复用 Redis 连接，减少连接开销
- 🛡️ **错误处理**: 自动重连和异常恢复
- 📊 **日志监控**: 详细的操作日志和错误信息
- 🔧 **优雅关闭**: 支持信号处理和优雅关闭

## 🏗️ 架构设计

```
客户端应用 <---> Redis Proxy <---> Redis 服务器
    |              |                    |
    |              |                    |
    TCP连接     命令解析+转发         实际存储
```

## 📝 核心组件

### ProxyHandler
- **功能**: 处理客户端连接和命令转发
- **特点**: 
  - 解析 RESP 协议
  - 管理 Redis 连接池
  - 异步转发命令和响应

### ProxyServer  
- **功能**: TCP 服务器和生命周期管理
- **特点**:
  - 监听客户端连接
  - 优雅关闭处理
  - 信号捕获和处理

## 🚀 使用方法

### 1. 启动代理服务器

```bash
# 使用默认配置启动
go run .

# 启动后看到类似输出：
# 🚀 Redis Proxy Server starting on :8080
# 📡 Forwarding to Redis server: localhost:6379
```

### 2. 连接到代理

使用任何 Redis 客户端连接到代理：

```bash
# 使用 redis-cli
redis-cli -h localhost -p 8080

# 使用 telnet 测试
telnet localhost 8080
```

### 3. 测试代理功能

```bash
# 运行测试客户端
go run example/proxy_test.go

# 或指定代理地址
go run example/proxy_test.go localhost:8080
```

## 📊 配置文件

代理使用 `etc/config.yaml` 中的配置：

```yaml
server:
  port: 8080              # 代理监听端口
  max_connections: 10000  # 最大连接数

redis:
  host: localhost         # Redis 服务器地址
  port: 6379             # Redis 服务器端口
  pool_size: 100         # 连接池大小
```

## 🧪 测试场景

测试客户端涵盖以下场景：

1. **基础命令**: PING, SET, GET
2. **数据类型**: 字符串、数字、JSON
3. **键管理**: DEL, EXISTS, KEYS
4. **错误处理**: 不存在的键、非法命令
5. **并发**: 多个命令并发执行

## 🔧 扩展功能

### 添加新的命令支持

在 `forwardCommand` 方法中添加特殊处理逻辑：

```go
func (h *ProxyHandler) forwardCommand(clientConn *connection.Connection, redisClient *client.Client, command protocol.Reply) error {
    // 检查特定命令
    if multiBulkCmd, ok := command.(*protocol.MultiBulkReply); ok {
        if len(multiBulkCmd.Args) > 0 {
            cmdName := string(multiBulkCmd.Args[0])
            
            // 特殊处理某些命令
            switch strings.ToUpper(cmdName) {
            case "INFO":
                // 自定义 INFO 响应
                return h.handleInfoCommand(clientConn)
            case "MONITOR":
                // 监控命令特殊处理
                return h.handleMonitorCommand(clientConn, redisClient)
            }
        }
    }
    
    // 默认转发逻辑
    // ...
}
```

### 添加缓存层

可以在转发前检查本地缓存：

```go
// 检查本地缓存
if cachedResult := h.cache.Get(key); cachedResult != nil {
    _, err := clientConn.Write(cachedResult)
    return err
}

// 转发到 Redis 并缓存结果
result := redisClient.Send(multiBulkCmd.Args)
h.cache.Set(key, result.ToBytes())
```

## 📈 性能优化

1. **连接复用**: 使用连接池避免频繁建立连接
2. **异步处理**: 每个客户端连接独立协程处理  
3. **内存优化**: 零拷贝的字节操作
4. **错误恢复**: 自动重连和故障恢复

## 🐛 故障排除

### 常见问题

1. **连接拒绝**: 检查 Redis 服务器是否启动
2. **端口占用**: 修改配置中的端口号
3. **权限错误**: 检查 Redis 服务器认证配置

### 调试方法

启用调试日志：

```go
// 在 main.go 中设置
logx.SetLevel(logx.DebugLevel)
```

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📄 许可证

MIT License
