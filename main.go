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
	var configFile = flag.String("c", "etc/config-goredis-v2-fixed.yaml", "Configuration file path")
	flag.Parse()

	// Load configuration
	var c config.Config
	if err := conf.Load(*configFile, &c); err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

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
	} else if c.Server.UseGnet {
		serverType = "High-Performance (gnet)"
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
	} else if c.Server.UseAffinity {
		// Show affinity-specific configuration
		fmt.Printf("├── Max Client Connections: %d\n", c.Server.MaxConnections)
		fmt.Printf("├── Idle Timeout: %s\n", proxyConfig.MaxIdleTime)
		fmt.Printf("├── WATCH Commands: ✅ Fully Supported\n")
		fmt.Printf("└── Transaction Commands: ✅ MULTI/EXEC Supported\n")
	} else {
		// Show pool-specific configuration
		fmt.Printf("├── Max Idle Time: %s\n", proxyConfig.MaxIdleTime)
		fmt.Printf("├── Pool Max Idle: %d\n", proxyConfig.PoolMaxIdle)
		fmt.Printf("├── Pool Max Active: %d\n", proxyConfig.PoolMaxActive)
		fmt.Printf("└── Pool Conn Timeout: %s\n", proxyConfig.PoolConnTimeout)
	}

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
	} else if c.Server.UseGnet {
		// Use gnet high-performance server
		server, err := proxy.NewGNetServer(proxyConfig)
		if err != nil {
			log.Fatalf("Failed to create gnet proxy server: %v", err)
		}

		logx.Info("High-performance Redis Proxy with gnet starting...")
		if err := server.ListenAndServe(); err != nil {
			log.Fatalf("Failed to start gnet proxy server: %v", err)
		}
	} else {
		// Use traditional server
		server, err := proxy.NewServer(proxyConfig)
		if err != nil {
			log.Fatalf("Failed to create traditional proxy server: %v", err)
		}

		logx.Info("Traditional Redis Proxy with Connection Pool starting...")
		if err := server.ListenAndServe(); err != nil {
			log.Fatalf("Failed to start traditional proxy server: %v", err)
		}
	}
}
