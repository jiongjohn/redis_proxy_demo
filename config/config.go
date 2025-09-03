package config

// Config represents the application configuration
type Config struct {
	Server struct {
		Port              int    `json:",default=6380"`
		MaxConnections    int    `json:",default=1000"`
		ReadTimeout       string `json:",default=30s"`
		WriteTimeout      string `json:",default=30s"`
		IdleTimeout       string `json:",default=300s"`
		UseGnet           bool   `json:"use_gnet,default=false" yaml:"use_gnet"`                         // Use gnet for high-performance server
		UseAffinity       bool   `json:"use_affinity,default=false" yaml:"use_affinity"`                 // Use connection affinity for stateful commands
		UseGoRedis        bool   `json:"use_goredis,default=false" yaml:"use_goredis"`                   // Use go-redis with session management
		UseGoRedisV2      bool   `json:"use_goredis_v2,default=false" yaml:"use_goredis_v2"`             // Use go-redis V2 with proto package enhancement
		UseGoRedisV2Fixed bool   `json:"use_goredis_v2_fixed,default=false" yaml:"use_goredis_v2_fixed"` // Use go-redis V2 Fixed (direct RESP responses)
	}

	// ConnectionAffinity configuration for connection affinity mode
	ConnectionAffinity struct {
		MaxConnections      int    `json:",default=1000"`  // Maximum concurrent connections
		IdleTimeout         string `json:",default=5m"`    // Idle connection timeout
		HealthCheckInterval string `json:",default=30s"`   // Health check interval
		BufferSize          int    `json:",default=32768"` // Buffer size for data forwarding (32KB)
		StatsInterval       string `json:",default=30s"`   // Statistics reporting interval
	}

	// GoRedis configuration for go-redis mode
	GoRedis struct {
		PoolSize       int    `json:",default=10"`  // Redis connection pool size
		MinIdleConns   int    `json:",default=2"`   // Minimum idle connections
		MaxConnAge     string `json:",default=1h"`  // Maximum connection age
		IdleTimeout    string `json:",default=30m"` // Connection idle timeout
		DialTimeout    string `json:",default=10s"` // Connection dial timeout
		ReadTimeout    string `json:",default=5s"`  // Read timeout
		WriteTimeout   string `json:",default=5s"`  // Write timeout
		SessionTimeout string `json:",default=1h"`  // Client session timeout
		StatsInterval  string `json:",default=60s"` // Statistics reporting interval
	}

	Redis struct {
		Host               string `json:",default=localhost"`
		Port               int    `json:",default=6379"`
		Password           string `json:",optional"`
		Database           int    `json:",default=0"`
		PoolSize           int    `json:",default=10"`
		MinIdleConnections int    `json:",default=2"`
		MaxIdleConnections int    `json:",default=5"`
		ConnectionTimeout  string `json:",default=5s"`
		ReadTimeout        string `json:",default=3s"`
		WriteTimeout       string `json:",default=3s"`
	}

	Cache struct {
		Enabled         bool   `json:",default=true"`
		MaxSize         string `json:",default=256MB"`
		TTL             string `json:",default=300s"`
		EvictionPolicy  string `json:",default=lru"`
		CleanupInterval string `json:",default=60s"`
	}

	Kafka struct {
		Enabled           bool     `json:",default=true"`
		Brokers           []string `json:",default=[localhost:9092]"`
		Topic             string   `json:",default=redis-cache-events"`
		GroupID           string   `json:",default=redis-proxy-group"`
		AutoOffsetReset   string   `json:",default=latest"`
		SessionTimeout    string   `json:",default=30s"`
		HeartbeatInterval string   `json:",default=3s"`
	}

	Monitoring struct {
		Enabled         bool   `json:",default=true"`
		BigKeyThreshold string `json:",default=10MB"`
		StatsInterval   string `json:",default=60s"`
		MetricsPort     int    `json:",default=8080"`
		HealthCheckPath string `json:",default=/health"`
		MetricsPath     string `json:",default=/metrics"`
	}
}

// ServerConfig represents server configuration (for godis compatibility)
type ServerConfig struct {
	ClusterEnable bool
	// Add server config fields as needed
}

// Global configuration instance
var (
	// Properties for godis compatibility
	Properties *ServerConfig
	// AppConfig for application configuration
	AppConfig *Config
)

// Init initializes the configuration
func Init() {
	Properties = &ServerConfig{}
	AppConfig = &Config{}
}

// LoadConfig loads configuration from file
func LoadConfig(configPath string) (*Config, error) {
	return AppConfig, nil
}
