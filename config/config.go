package config

// Config represents the application configuration
type Config struct {
	Server struct {
		Port           int    `json:",default=6380"`
		MaxConnections int    `json:",default=1000"`
		ReadTimeout    string `json:",default=30s"`
		WriteTimeout   string `json:",default=30s"`
		IdleTimeout    string `json:",default=300s"`
		UseGnet        bool   `json:"use_gnet,default=false"` // Use gnet for high-performance server
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
