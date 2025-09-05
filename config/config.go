package config

// Config represents the application configuration
type Config struct {
	Server struct {
		Port                 int    `json:"port,default=6380"`
		MaxConnections       int    `json:"max_connections,default=1000"`
		ReadTimeout          string `json:"read_timeout,default=30s"`
		WriteTimeout         string `json:"write_timeout,default=30s"`
		IdleTimeout          string `json:"idle_timeout,default=300s"`
		UseGnet              bool   `json:"use_gnet,default=false"`               // Use gnet for high-performance server
		UseAffinity          bool   `json:"use_affinity,default=false"`           // Use connection affinity for stateful commands
		UseGoRedis           bool   `json:"use_goredis,default=false"`            // Use go-redis with session management
		UseGoRedisV2         bool   `json:"use_goredis_v2,default=false"`         // Use go-redis V2 with proto package enhancement
		UseGoRedisV2Fixed    bool   `json:"use_goredis_v2_fixed,default=false"`   // Use go-redis V2 Fixed (direct RESP responses)
		UseIntelligentPool   bool   `json:"use_intelligent_pool,default=false"`   // Use intelligent connection pooling based on command classification
		UseOptimizedAffinity bool   `json:"use_optimized_affinity,default=false"` // Use optimized connection affinity with pre-connection pool
	} `json:"server"`

	// ConnectionAffinity configuration for connection affinity mode
	ConnectionAffinity struct {
		MaxConnections      int    `json:"max_connections,default=1000"`      // Maximum concurrent connections
		PrePoolSize         int    `json:"pre_pool_size,default=100"`         // Pre-connection pool size
		PrewarmConnections  int    `json:"prewarm_connections,default=20"`    // Number of connections to prewarm
		IdleTimeout         string `json:"idle_timeout,default=5m"`           // Idle connection timeout
		HealthCheckInterval string `json:"health_check_interval,default=30s"` // Health check interval
		BufferSize          int    `json:"buffer_size,default=32768"`         // Buffer size for data forwarding (32KB)
		ConnectTimeout      string `json:"connect_timeout,default=3s"`        // Connection timeout
		StatsInterval       string `json:"stats_interval,default=30s"`        // Statistics reporting interval
	} `json:"connection_affinity"`

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
	} `json:"goredis"`

	// IntelligentPool configuration for intelligent connection pooling mode
	IntelligentPool struct {
		MaxPoolSize        int    `json:"max_pool_size,default=10"`
		MinIdleConns       int    `json:"min_idle_conns,default=1"`
		MaxIdleConns       int    `json:"max_idle_conns,default=5"`
		IdleTimeout        string `json:"idle_timeout,default=5m"`
		MaxLifetime        string `json:"max_lifetime,default=1h"`
		SessionTimeout     string `json:"session_timeout,default=30m"`
		CleanupInterval    string `json:"cleanup_interval,default=1m"`
		StatsInterval      string `json:"stats_interval,default=30s"`
		CommandTimeout     string `json:"command_timeout,default=30s"`
		MaxRetries         int    `json:"max_retries,default=3"`
		ConnectionHoldTime string `json:"connection_hold_time,default=30s"`
	} `json:"intelligent_pool"`

	Redis struct {
		Host               string `json:"host,default=localhost"`
		Port               int    `json:"port,default=6379"`
		Password           string `json:"password,optional"`
		Database           int    `json:"database,default=0"`
		PoolSize           int    `json:"pool_size,default=10"`
		MinIdleConnections int    `json:"min_idle_connections,default=2"`
		MaxIdleConnections int    `json:"max_idle_connections,default=5"`
		ConnectionTimeout  string `json:"connection_timeout,default=5s"`
		ReadTimeout        string `json:"read_timeout,default=3s"`
		WriteTimeout       string `json:"write_timeout,default=3s"`
	} `json:"redis"`

	Cache struct {
		Enabled         bool   `json:",default=true"`
		MaxSize         string `json:",default=256MB"`
		TTL             string `json:",default=300s"`
		EvictionPolicy  string `json:",default=lru"`
		CleanupInterval string `json:",default=60s"`
	} `json:"cache"`

	Kafka struct {
		Enabled           bool     `json:",default=true"`
		Brokers           []string `json:",default=[localhost:9092]"`
		Topic             string   `json:",default=redis-cache-events"`
		GroupID           string   `json:",default=redis-proxy-group"`
		AutoOffsetReset   string   `json:",default=latest"`
		SessionTimeout    string   `json:",default=30s"`
		HeartbeatInterval string   `json:",default=3s"`
	} `json:"kafka"`

	Monitoring struct {
		Enabled         bool   `json:",default=true"`
		BigKeyThreshold string `json:",default=10MB"`
		StatsInterval   string `json:",default=60s"`
		MetricsPort     int    `json:",default=8080"`
		HealthCheckPath string `json:",default=/health"`
		MetricsPath     string `json:",default=/metrics"`
	} `json:"monitoring"`
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
