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
	fmt.Printf("├── Cache: %s (Max Size: %s, Policy: %s)\n",
		func() string {
			if c.Cache.Enabled {
				return "Enabled"
			} else {
				return "Disabled"
			}
		}(),
		c.Cache.MaxSize, c.Cache.EvictionPolicy)
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

	if c.Server.UseIntelligentPool {
		serverType = "Intelligent Connection Pool Proxy"
		connectionMode = "NORMAL/INIT/SESSION Command Classification"
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

	if c.Server.UseIntelligentPool {
		// Show intelligent pool specific configuration
		fmt.Printf("├── Max Pool Size: %d\n", c.IntelligentPool.MaxPoolSize)
		fmt.Printf("├── Min Idle Connections: %d\n", c.IntelligentPool.MinIdleConns)
		fmt.Printf("├── Max Idle Connections: %d\n", c.IntelligentPool.MaxIdleConns)
		fmt.Printf("├── Session Timeout: %s\n", c.IntelligentPool.SessionTimeout)
		fmt.Printf("├── Command Classification: ✅ NORMAL/INIT/SESSION\n")
		fmt.Printf("├── WATCH Commands: ✅ Fully Supported\n")
		fmt.Printf("├── Transaction Support: ✅ MULTI/EXEC with Session State\n")
		fmt.Printf("└── Smart Resource Management: ✅ Context-based Pooling\n")
	} else if c.Server.UseOptimizedAffinity {
		// Show optimized affinity-specific configuration
		fmt.Printf("├── Max Client Connections: %d\n", c.ConnectionAffinity.MaxConnections)
		fmt.Printf("├── Pre-Connection Pool Size: %d\n", c.ConnectionAffinity.PrePoolSize)
		fmt.Printf("├── Prewarm Connections: %d\n", c.ConnectionAffinity.PrewarmConnections)
		fmt.Printf("├── Idle Timeout: %s\n", c.ConnectionAffinity.IdleTimeout)
		fmt.Printf("├── Connect Timeout: %s\n", c.ConnectionAffinity.ConnectTimeout)
		fmt.Printf("├── WATCH Commands: ✅ Fully Supported\n")
		fmt.Printf("├── Transaction Commands: ✅ MULTI/EXEC Supported\n")
		fmt.Printf("└── Zero Connection Latency: ✅ Pre-Connection Pool\n")
	} else if c.Server.UseAffinity {
		// Show traditional affinity-specific configuration
		fmt.Printf("├── Max Client Connections: %d\n", c.Server.MaxConnections)
		fmt.Printf("├── Idle Timeout: %s\n", proxyConfig.MaxIdleTime)
		fmt.Printf("├── WATCH Commands: ✅ Fully Supported\n")
		fmt.Printf("└── Transaction Commands: ✅ MULTI/EXEC Supported\n")
	}

	fmt.Printf("\n🚀 Starting Redis Proxy Server...path:%s,config :%+v \n", *configFile, c.IntelligentPool)
	// Create appropriate server based on configuration
	if c.Server.UseIntelligentPool {
		// Parse IntelligentPool configuration durations
		idleTimeout, _ := time.ParseDuration(c.IntelligentPool.IdleTimeout)
		maxLifetime, _ := time.ParseDuration(c.IntelligentPool.MaxLifetime)
		cleanupInterval, _ := time.ParseDuration(c.IntelligentPool.CleanupInterval)
		sessionTimeout, _ := time.ParseDuration(c.IntelligentPool.SessionTimeout)
		commandTimeout, _ := time.ParseDuration(c.IntelligentPool.CommandTimeout)
		connectionHoldTime, _ := time.ParseDuration(c.IntelligentPool.ConnectionHoldTime)

		// Use intelligent connection pool server with detailed configuration
		server, err := proxy.NewPoolServerWithDetailedConfig(
			proxyConfig,
			c.IntelligentPool.MaxPoolSize,
			c.IntelligentPool.MinIdleConns,
			c.IntelligentPool.MaxIdleConns,
			idleTimeout,
			maxLifetime,
			cleanupInterval,
			sessionTimeout,
			commandTimeout,
			connectionHoldTime,
		)
		if err != nil {
			log.Fatalf("Failed to create intelligent pool proxy server: %v", err)
		}

		logx.Info("🚀 Intelligent Connection Pool Proxy starting...")
		logx.Info("✅ Command Classification: NORMAL/INIT/SESSION")
		logx.Info("✅ Smart Connection Reuse Based on Context")
		logx.Info("✅ Optimized Resource Management")
		logx.Info(fmt.Sprintf("🔗 Connection Hold Time: %v", connectionHoldTime))
		logx.Info("🧠 Proto Library RESP Parsing")
		if err := server.ListenAndServe(); err != nil {
			log.Fatalf("Failed to start intelligent pool proxy server: %v", err)
		}
	} else if c.Server.UseOptimizedAffinity {
		// Parse optimized affinity configuration durations
		idleTimeout, _ := time.ParseDuration(c.ConnectionAffinity.IdleTimeout)
		healthCheckInterval, _ := time.ParseDuration(c.ConnectionAffinity.HealthCheckInterval)

		// Use optimized affinity server with pre-connection pool
		server, err := proxy.NewOptimizedAffinityServerWithFullConfig(
			proxyConfig,
			proxyConfig.RedisAddr,
			proxyConfig.RedisPassword,
			c.ConnectionAffinity.MaxConnections,
			c.ConnectionAffinity.PrePoolSize,
			c.ConnectionAffinity.PrewarmConnections,
			idleTimeout,
			healthCheckInterval,
		)
		if err != nil {
			log.Fatalf("Failed to create optimized affinity proxy server: %v", err)
		}

		logx.Info("🚀 优化的连接亲和性Redis代理启动中...")
		logx.Info("✅ WATCH/MULTI/EXEC命令完全支持")
		logx.Info("⚡ 零连接建立延迟 - 预连接池优化")
		logx.Info(fmt.Sprintf("🏊 预连接池大小: %d", c.ConnectionAffinity.PrePoolSize))
		logx.Info(fmt.Sprintf("🔥 预热连接数: %d", c.ConnectionAffinity.PrewarmConnections))
		if err := server.ListenAndServe(); err != nil {
			log.Fatalf("Failed to start optimized affinity proxy server: %v", err)
		}
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
