package config

// Config represents the application configuration
type Config struct {
	Server struct {
		Port              int    `json:"port,default=6380"`
		MaxConnections    int    `json:"max_connections,default=1000"`
		ReadTimeout       string `json:"read_timeout,default=30s"`
		WriteTimeout      string `json:"write_timeout,default=30s"`
		IdleTimeout       string `json:"idle_timeout,default=300s"`
		UseAffinity       bool   `json:"use_affinity,default=false"`        // Use connection affinity for stateful commands
		UseDedicatedProxy bool   `json:"use_dedicated_proxy,default=false"` // Use dedicated connection pool proxy with 1:1 client-redis binding
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

	// DedicatedProxy configuration for dedicated connection pool mode
	DedicatedProxy struct {
		MaxConnections    int    `json:"max_connections,default=100"`                       // Maximum Redis connections in pool
		InitConnections   int    `json:"init_connections,default=10"`                       // Initial connections to create
		WaitTimeout       string `json:"wait_timeout,default=5s"`                           // Timeout when waiting for available connection
		IdleTimeout       string `json:"idle_timeout,default=5m"`                           // Connection idle timeout
		SessionTimeout    string `json:"session_timeout,default=10m"`                       // Client session timeout
		CommandTimeout    string `json:"command_timeout,default=30s"`                       // Redis command execution timeout
		DefaultDatabase   int    `json:"default_database,default=0"`                        // Default Redis database
		DefaultClientName string `json:"default_client_name,default=redis-proxy-dedicated"` // Default client name
		StatsInterval     string `json:"stats_interval,default=30s"`                        // Statistics reporting interval
	} `json:"dedicated_proxy"`

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
		Enabled         bool   `json:"enabled,default=true"`
		MaxSize         int    `json:"max_size,default=10000"`       // Maximum number of cached items
		TTL             string `json:"ttl,default=60s"`              // Time to live for cached items
		EnableTTL       bool   `json:"enable_ttl,default=true"`      // Enable TTL-based expiration
		EvictionPolicy  string `json:"eviction_policy,default=lru"`  // Eviction policy (lru, lfu, etc.)
		EnableStats     bool   `json:"enable_stats,default=true"`    // Enable cache statistics
		CleanupInterval string `json:"cleanup_interval,default=60s"` // Cleanup interval for expired items
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
