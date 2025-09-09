# Redis Proxy Demo

一个高性能、功能完整的 Redis 代理服务器，支持多种连接模式和高级特性。

[![Go Version](https://img.shields.io/badge/Go-1.24.2-blue.svg)](https://golang.org/)
[![Redis](https://img.shields.io/badge/Redis-7.0+-red.svg)](https://redis.io/)

## 🌟 核心特性

### 🔗 多种连接模式
- **专用连接池代理** (Dedicated Connection Pool) - 1:1 客户端-Redis 连接绑定
- **连接亲和性代理** (Connection Affinity) - 支持 WATCH/MULTI/EXEC 事务
- **传统连接池** - 高并发场景下的连接复用

### 🚀 高级功能
- ✅ **完整 Redis 协议支持** (RESP2/RESP3)
- ✅ **事务命令支持** (MULTI/EXEC/WATCH/DISCARD)
- ✅ **大 Key 处理优化** (>10MB 数据传输)
- ✅ **智能连接池管理** (预分配+动态扩展)
- ✅ **会话隔离** (完整的客户端状态管理)
- ✅ **性能监控** (Prometheus 指标)
- ✅ **健康检查** (连接状态监控)

### 📊 性能优化
- **零拷贝数据转发** - 最小化内存分配
- **连接复用** - 减少连接建立开销
- **异步处理** - 非阻塞 I/O 操作
- **缓冲区优化** - 可配置的缓冲区大小

## 🏗️ 架构设计

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Redis Client  │────│  Redis Proxy    │────│   Redis Server  │
│                 │    │                 │    │                 │
│ - go-redis      │    │ - 连接池管理     │    │ - 数据存储      │
│ - jedis         │    │ - 协议转换       │    │ - 命令执行      │
│ - node_redis    │    │ - 会话管理       │    │ - 持久化        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### 专用连接池模式架构

```
Client 1 ──┐
           ├── Proxy ──┐
Client 2 ──┘           ├── Redis Pool ── Redis Server
                       │  ┌─────────┐
Client 3 ──┐           │  │ Conn 1  │
           ├── Proxy ──┘  │ Conn 2  │
Client 4 ──┘              │ Conn N  │
                          └─────────┘
```

## 🚀 快速开始

### 环境要求

- Go 1.24.2+
- Redis 6.0+
- Docker & Docker Compose (可选)

### 安装与运行

#### 1. 克隆项目
```bash
git clone <repository-url>
cd redis_proxy_demo
```

#### 2. 安装依赖
```bash
go mod tidy
```

#### 3. 启动 Redis 服务器
```bash
# 方式1: 直接启动
redis-server

# 方式2: 使用 Docker
docker-compose -f docker-compose-env.yml up redis
```

#### 4. 编译代理服务器
```bash
go build -o redis-proxy-demo .
```

#### 5. 启动代理服务器

**专用连接池模式** (推荐):
```bash
./redis-proxy-demo -c etc/config-dedicated-proxy.yaml
```

**连接亲和性模式** (支持事务):
```bash
./redis-proxy-demo -c etc/config-affinity.yaml
```

### 验证运行

```bash
# 连接到代理服务器 (端口 6380)
redis-cli -p 6380

# 测试基本命令
127.0.0.1:6380> SET test "Hello Redis Proxy"
OK
127.0.0.1:6380> GET test
"Hello Redis Proxy"
```

## 📋 配置说明

### 专用连接池配置 (`config-dedicated-proxy.yaml`)

```yaml
server:
  port: 6380                    # 代理监听端口
  use_dedicated_proxy: true     # 启用专用连接池

dedicated_proxy:
  max_connections: 200          # 最大 Redis 连接数
  init_connections: 20          # 初始连接数
  wait_timeout: "2s"           # 获取连接等待超时
  idle_timeout: "3m"           # 连接空闲超时
  session_timeout: "5m"        # 客户端会话超时
  command_timeout: "10s"       # 命令执行超时

redis:
  host: "localhost"
  port: 6379
  password: ""                 # Redis 密码
```

### 连接亲和性配置 (`config-affinity.yaml`)

```yaml
server:
  port: 6380
  use_affinity: true           # 启用连接亲和性

connection_affinity:
  max_connections: 1000        # 最大并发连接
  idle_timeout: "5m"          # 空闲超时
  buffer_size: 32768          # 缓冲区大小 (32KB)
```

## 🧪 测试与演示

### 运行客户端演示

```bash
cd example/go-redis-client-demo
go run main.go
```

演示包含:
- ✅ 基础 SET/GET 操作
- ✅ 大 Key 处理 (10KB+ 数据)
- ✅ 数据类型操作 (List/Hash/Set)
- ✅ 事务操作 (MULTI/EXEC)
- ✅ 管道操作 (Pipeline)
- ✅ 性能测试

### 性能基准测试

```bash
# GET/SET 性能测试
go run benchmarks/getset_benchmark.go

# 连接池性能测试
go run benchmarks/connection_pool_benchmark.go

# 使用脚本测试
./benchmark_test.sh
```

### Docker 环境测试

```bash
# 启动完整测试环境
docker-compose -f docker-compose-env.yml up

# 运行基准测试
docker-compose -f docker-compose-env.yml --profile benchmark up

# 启动 Redis 管理界面
docker-compose -f docker-compose-env.yml --profile ui up redis-commander
# 访问: http://localhost:8082 (admin/admin123)
```

## 🔧 开发指南

### 项目结构

```
redis_proxy_demo/
├── main.go                 # 主程序入口
├── config/                 # 配置管理
│   └── config.go
├── proxy/                  # 代理核心实现
│   ├── dedicated_handler.go    # 专用连接池处理器
│   ├── dedicated_pool.go       # 专用连接池
│   ├── affinity_handler.go     # 连接亲和性处理器
│   └── affinity_server.go      # 亲和性服务器
├── redis/                  # Redis 协议实现
│   ├── proto/              # 协议解析
│   └── util/               # 工具函数
├── lib/                    # 公共库
│   ├── logger/             # 日志系统
│   ├── pool/               # 连接池
│   ├── timewheel/          # 时间轮
│   └── utils/              # 工具函数
├── example/                # 客户端演示
├── benchmarks/             # 性能测试
├── etc/                    # 配置文件
└── docker/                 # Docker 配置
```

## 🐛 故障排除

### 常见问题

**Q: 连接被拒绝**
```
dial tcp :6380: connect: connection refused
```
A: 确保代理服务器正在运行，检查端口配置

**Q: 大 Key 数据不完整**
```
❌ 大key数据不完整! 期望: 10240, 实际: 4096
```
A: 检查缓冲区配置，增加 `buffer_size` 设置

**Q: 事务命令失败**
```
EXECABORT Transaction discarded because of previous errors
```
A: 使用连接亲和性模式 (`use_affinity: true`)

**Q: 性能异常低**
```
SET: 2.5s (40 ops/sec)
```
A: 检查网络延迟，调整连接池配置

### 日志分析

```bash
# 查看代理日志
tail -f proxy_*.log

# 过滤错误日志
grep "ERROR\|WARN" proxy_*.log

# 监控连接状态
grep "connection" proxy_*.log
```


### 性能调优

1. **连接池优化**:
   ```yaml
   dedicated_proxy:
     max_connections: 500      # 根据并发需求调整
     init_connections: 50      # 预热连接数
   ```

2. **缓冲区调优**:
   ```yaml
   connection_affinity:
     buffer_size: 65536        # 64KB，适合大 Key
   ```

3. **超时设置**:
   ```yaml
   dedicated_proxy:
     command_timeout: "5s"     # 根据业务调整
     wait_timeout: "1s"        # 快速失败
   ```

### 代码规范

- 使用 `gofmt` 格式化代码
- 添加必要的注释和文档
- 编写单元测试
- 遵循 Go 语言最佳实践