package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"redis-proxy-demo/config"
	"redis-proxy-demo/proxy"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
)

func main() {
	fmt.Println("🚀 Redis Proxy Demo - Starting...")

	// Parse command line arguments
	var configFile = flag.String("c", "etc/config-intelligent-pool.yaml", "Configuration file path")
	flag.Parse()

	// Load configuration
	var c config.Config
	conf.MustLoad(*configFile, &c)
	// Initialize logging
	logx.Info("Configuration loaded successfully")

	// Print configuration summary
	fmt.Printf("✅ Configuration Summary:\n")
	fmt.Printf("├── Server: %s:%d (Max Connections: %d)\n", "localhost", c.Server.Port, c.Server.MaxConnections)
	fmt.Printf("├── Redis: %s:%d (Pool Size: %d)\n", c.Redis.Host, c.Redis.Port, c.Redis.PoolSize)
	fmt.Printf("├── Cache: %s (Max Size: %dMB, Policy: %s)\n",
		func() string {
			if c.Cache.Enabled {
				return "Enabled"
			} else {
				return "Disabled"
			}
		}(),
		c.Cache.HardMaxCacheSize, c.Cache.EvictionPolicy)
	fmt.Printf("├── Kafka: %s (Brokers: %v)\n",
		func() string {
			if c.Kafka.Enabled {
				return "Enabled"
			} else {
				return "Disabled"
			}
		}(),
		c.Kafka.Brokers)
	fmt.Printf("└── Monitoring: %s (Port: %d, Big Key: %s)\n",
		func() string {
			if c.Monitoring.Enabled {
				return "Enabled"
			} else {
				return "Disabled"
			}
		}(),
		c.Monitoring.MetricsPort, c.Monitoring.BigKeyThreshold)

	// Create proxy server configuration with connection pool settings
	proxyConfig := proxy.Config{
		Address:         fmt.Sprintf(":%d", c.Server.Port),
		RedisAddr:       fmt.Sprintf("%s:%d", c.Redis.Host, c.Redis.Port),
		RedisPassword:   c.Redis.Password, // Redis authentication password
		MaxIdleTime:     5 * time.Minute,  // Default idle timeout
		PoolMaxIdle:     20,               // Maximum idle connections in pool
		PoolMaxActive:   100,              // Maximum active connections in pool
		PoolConnTimeout: 10 * time.Second, // Connection timeout
	}

	// Determine server type based on configuration
	serverType := "Traditional"
	connectionMode := "Connection Pool"

	if c.Server.UseDedicatedProxy {
		serverType = "Dedicated Connection Pool Proxy"
		connectionMode = "1:1 Client-Redis Binding with Pool Management"
	} else if c.Server.UseAffinity {
		serverType = "Connection Affinity"
		connectionMode = "1:1 Connection Mapping"
	}

	fmt.Printf("\n🚀 Starting %s Redis Proxy Server...\n", serverType)
	fmt.Printf("├── Server Type: %s\n", serverType)
	fmt.Printf("├── Connection Mode: %s\n", connectionMode)
	fmt.Printf("├── Proxy Address: %s\n", proxyConfig.Address)
	fmt.Printf("├── Redis Target: %s\n", proxyConfig.RedisAddr)
	fmt.Printf("├── Redis Auth: %s\n", func() string {
		if proxyConfig.RedisPassword != "" {
			return "Enabled (***)"
		}
		return "Disabled"
	}())

	if c.Server.UseDedicatedProxy {
		// Show dedicated proxy specific configuration
		fmt.Printf("├── Max Connections: %d\n", c.DedicatedProxy.MaxConnections)
		fmt.Printf("├── Init Connections: %d\n", c.DedicatedProxy.InitConnections)
		fmt.Printf("├── Wait Timeout: %s\n", c.DedicatedProxy.WaitTimeout)
		fmt.Printf("├── Idle Timeout: %s\n", c.DedicatedProxy.IdleTimeout)
		fmt.Printf("├── Session Timeout: %s\n", c.DedicatedProxy.SessionTimeout)
		fmt.Printf("├── Command Timeout: %s\n", c.DedicatedProxy.CommandTimeout)
		fmt.Printf("├── Default Database: %d\n", c.DedicatedProxy.DefaultDatabase)
		fmt.Printf("├── Client-Redis Binding: ✅ 1:1 Dedicated Connection\n")
		fmt.Printf("├── Connection Pool Management: ✅ Pre-allocated + Dynamic\n")
		fmt.Printf("├── Redis Protocol Support: ✅ RESP2/RESP3 + Full Commands\n")
		fmt.Printf("├── Session Isolation: ✅ Complete Client Isolation\n")
		fmt.Printf("└── Resource Control: ✅ Strict Connection Limits\n")
	} else if c.Server.UseAffinity {
		// Show traditional affinity-specific configuration
		fmt.Printf("├── Max Client Connections: %d\n", c.Server.MaxConnections)
		fmt.Printf("├── Idle Timeout: %s\n", proxyConfig.MaxIdleTime)
		fmt.Printf("├── WATCH Commands: ✅ Fully Supported\n")
		fmt.Printf("└── Transaction Commands: ✅ MULTI/EXEC Supported\n")
	}

	// Create appropriate server based on configuration
	if c.Server.UseDedicatedProxy {
		// Parse DedicatedProxy configuration durations
		waitTimeout, _ := time.ParseDuration(c.DedicatedProxy.WaitTimeout)
		idleTimeout, _ := time.ParseDuration(c.DedicatedProxy.IdleTimeout)
		sessionTimeout, _ := time.ParseDuration(c.DedicatedProxy.SessionTimeout)
		commandTimeout, _ := time.ParseDuration(c.DedicatedProxy.CommandTimeout)

		// Create dedicated proxy server configuration
		dedicatedConfig := proxy.DedicatedServerConfig{
			ListenAddr:        fmt.Sprintf(":%d", c.Server.Port),
			RedisAddr:         fmt.Sprintf("%s:%d", c.Redis.Host, c.Redis.Port),
			RedisPassword:     c.Redis.Password,
			MaxConnections:    c.DedicatedProxy.MaxConnections,
			InitConnections:   c.DedicatedProxy.InitConnections,
			WaitTimeout:       waitTimeout,
			IdleTimeout:       idleTimeout,
			SessionTimeout:    sessionTimeout,
			CommandTimeout:    commandTimeout,
			DefaultDatabase:   c.DedicatedProxy.DefaultDatabase,
			DefaultClientName: c.DedicatedProxy.DefaultClientName,
			CacheConfig:       &c, // 传递完整配置以支持缓存
		}

		// Create dedicated proxy server
		server, err := proxy.NewDedicatedServer(dedicatedConfig)
		if err != nil {
			log.Fatalf("Failed to create dedicated proxy server: %v", err)
		}

		logx.Info("🚀 专用连接池Redis代理启动中...")
		logx.Info("✅ 1:1客户端-Redis连接绑定")
		logx.Info("✅ 完整的Redis协议支持 (RESP2/RESP3)")
		logx.Info("✅ 智能连接池管理 (预分配+动态扩展)")
		logx.Info("✅ 严格的资源控制和会话隔离")
		logx.Info(fmt.Sprintf("🏊 最大连接数: %d", c.DedicatedProxy.MaxConnections))
		logx.Info(fmt.Sprintf("🔥 初始连接数: %d", c.DedicatedProxy.InitConnections))
		logx.Info(fmt.Sprintf("⏱️ 等待超时: %v", waitTimeout))
		logx.Info(fmt.Sprintf("💤 空闲超时: %v", idleTimeout))

		if err := server.Start(); err != nil {
			log.Fatalf("Failed to start dedicated proxy server: %v", err)
		}

		// Keep the server running
		select {}
	} else if c.Server.UseAffinity {
		// Use connection affinity server for WATCH command support
		server, err := proxy.NewAffinityServerWithFullConfig(
			proxyConfig,
			proxyConfig.RedisAddr,
			proxyConfig.RedisPassword,
			c.Server.MaxConnections,
			proxyConfig.MaxIdleTime,
			30*time.Second, // Health check interval
		)
		if err != nil {
			log.Fatalf("Failed to create affinity proxy server: %v", err)
		}

		logx.Info("Connection Affinity Redis Proxy starting...")
		logx.Info("✅ WATCH/MULTI/EXEC commands fully supported")
		if err := server.ListenAndServe(); err != nil {
			log.Fatalf("Failed to start affinity proxy server: %v", err)
		}
	}
	return
}
