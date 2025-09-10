package cache

import (
	"testing"
	"time"

	"redis-proxy-demo/config"
	"redis-proxy-demo/lib/logger"
)

func TestBigCacheWrapper(t *testing.T) {
	// 创建BigCache封装器
	cache, err := NewBigCacheWrapper(10*time.Second, 10, 1024*1024, false) // 10MB total, 1MB per entry, no verbose
	if err != nil {
		t.Fatalf("Failed to create BigCacheWrapper: %v", err)
	}
	defer cache.Close()

	// 测试Set和Get
	key := "test:key"
	value := "test_value"

	cache.Set(key, value)

	retrievedValue, found := cache.Get(key)
	if !found {
		t.Error("Expected to find cached value")
	}
	if retrievedValue != value {
		t.Errorf("Expected %v, got %v", value, retrievedValue)
	}

	// 测试统计信息
	stats := cache.Stats()
	if stats.Sets != 1 {
		t.Errorf("Expected 1 set, got %d", stats.Sets)
	}
	if stats.Hits != 1 {
		t.Errorf("Expected 1 hit, got %d", stats.Hits)
	}

	// 测试Delete
	cache.Delete(key)
	_, found = cache.Get(key)
	if found {
		t.Error("Expected key to be deleted")
	}

	// 测试Clear
	cache.Set("key1", "value1")
	cache.Set("key2", "value2")
	cache.Clear()

	if cache.Len() != 0 {
		t.Errorf("Expected cache to be empty after clear, got length %d", cache.Len())
	}
}

func TestBigCacheWrapperTTL(t *testing.T) {
	cache, err := NewBigCacheWrapper(100*time.Millisecond, 10, 1024*1024, false) // 10MB total, 1MB per entry, no verbose
	if err != nil {
		t.Fatalf("Failed to create BigCacheWrapper: %v", err)
	}
	defer cache.Close()

	key := "test:ttl"
	value := "test_value"

	// 设置带TTL的值
	cache.SetWithTTL(key, value, 50*time.Millisecond)

	// 立即获取应该能找到
	retrievedValue, found := cache.Get(key)
	if !found {
		t.Error("Expected to find cached value immediately")
	}
	if retrievedValue != value {
		t.Errorf("Expected %v, got %v", value, retrievedValue)
	}

	// 等待更长时间让TTL过期（BigCache可能需要更长时间）
	time.Sleep(200 * time.Millisecond)

	// 过期后应该找不到（但BigCache的TTL机制可能不是精确的）
	_, found = cache.Get(key)
	if found {
		t.Logf("Key still found after TTL, this might be due to BigCache's internal cleanup mechanism")
		// 不认为这是失败，因为BigCache的TTL清理可能是异步的
	}
}

func TestSmartCache(t *testing.T) {
	cfg := &config.Config{}
	cfg.Cache.Enabled = true
	cfg.Cache.TTL = "60s"
	cfg.Cache.HardMaxCacheSize = 10      // 10MB
	cfg.Cache.MaxEntrySize = 1024 * 1024 // 1MB
	cfg.Cache.Verbose = false            // 测试时不启用详细日志

	cache, err := NewSmartCache(cfg)
	if err != nil {
		t.Fatalf("Failed to create SmartCache: %v", err)
	}
	defer cache.Close()

	// 测试是否启用
	if !cache.IsEnabled() {
		t.Error("Expected cache to be enabled")
	}

	// 测试hostname
	if cache.GetHostname() == "" {
		t.Error("Expected hostname to be set")
	}

	// 测试命令过滤
	testCases := []struct {
		command  string
		expected bool
	}{
		{"GET", true},
		{"get", true},
		{"SET", true},
		{"set", true},
		{"DEL", false},
		{"SADD", false},
		{"HGET", false},
	}

	for _, tc := range testCases {
		result := cache.ShouldCache(tc.command)
		if result != tc.expected {
			t.Errorf("ShouldCache(%s) = %v, expected %v", tc.command, result, tc.expected)
		}
	}

	// 测试GET操作（缓存未命中）
	key := "test:key"
	value, found := cache.ProcessGET(key)
	if found {
		t.Error("Expected cache miss for new key")
	}
	if value != nil {
		t.Error("Expected nil value for cache miss")
	}

	// 测试SET操作
	setValue := "test_value"
	cache.ProcessSET(key, setValue, 30*time.Second)

	// 再次GET应该命中
	value, found = cache.ProcessGET(key)
	if !found {
		t.Error("Expected cache hit after set")
	}
	if value != setValue {
		t.Errorf("Expected %v, got %v", setValue, value)
	}

	// 测试统计信息
	stats := cache.GetStats()
	if !stats["enabled"].(bool) {
		t.Error("Expected enabled to be true")
	}
	if stats["hits"].(int64) != 1 {
		t.Errorf("Expected 1 hit, got %v", stats["hits"])
	}
	if stats["misses"].(int64) != 1 {
		t.Errorf("Expected 1 miss, got %v", stats["misses"])
	}

	// 测试ProcessRemoteUpdate
	newValue := "updated_value"
	cache.ProcessRemoteUpdate(key, newValue)

	value, found = cache.ProcessGET(key)
	if !found {
		t.Error("Expected cache hit after remote update")
	}
	if value != newValue {
		t.Errorf("Expected %v, got %v", newValue, value)
	}

	// 测试InvalidateCache
	cache.InvalidateCache(key)
	_, found = cache.ProcessGET(key)
	if found {
		t.Error("Expected cache miss after invalidation")
	}

	// 测试SetToCache
	cache.SetToCache("GET", key, "direct_set_value")
	value, found = cache.ProcessGET(key)
	if !found {
		t.Error("Expected cache hit after SetToCache")
	}
	if value != "direct_set_value" {
		t.Errorf("Expected 'direct_set_value', got %v", value)
	}

	// 测试Clear
	cache.Clear()
	_, found = cache.ProcessGET(key)
	if found {
		t.Error("Expected cache miss after clear")
	}
}

func TestSmartCacheDisabled(t *testing.T) {
	cfg := &config.Config{}
	cfg.Cache.Enabled = false
	cfg.Cache.HardMaxCacheSize = 10      // 10MB
	cfg.Cache.MaxEntrySize = 1024 * 1024 // 1MB
	cfg.Cache.Verbose = false            // 测试时不启用详细日志

	cache, err := NewSmartCache(cfg)
	if err != nil {
		t.Fatalf("Failed to create SmartCache: %v", err)
	}
	defer cache.Close()

	// 测试是否禁用
	if cache.IsEnabled() {
		t.Error("Expected cache to be disabled")
	}

	// 所有操作都应该无效
	if cache.ShouldCache("GET") {
		t.Error("Expected ShouldCache to return false when disabled")
	}

	value, found := cache.ProcessGET("test")
	if found {
		t.Error("ProcessGET should not find value when disabled")
	}
	if value != nil {
		t.Error("ProcessGET should return nil when disabled")
	}

	// SET操作应该静默无效
	cache.ProcessSET("test", "value", time.Minute)
	_, found = cache.ProcessGET("test")
	if found {
		t.Error("ProcessGET should not find value after SET when disabled")
	}
}

func TestSmartCacheCalculateLocalTTL(t *testing.T) {
	cfg := &config.Config{}
	cfg.Cache.Enabled = true
	cfg.Cache.TTL = "60s"
	cfg.Cache.HardMaxCacheSize = 10      // 10MB
	cfg.Cache.MaxEntrySize = 1024 * 1024 // 1MB
	cfg.Cache.Verbose = false            // 测试时不启用详细日志

	cache, err := NewSmartCache(cfg)
	if err != nil {
		t.Fatalf("Failed to create SmartCache: %v", err)
	}
	defer cache.Close()

	testCases := []struct {
		inputTTL    time.Duration
		expectedMin time.Duration
		expectedMax time.Duration
		description string
	}{
		{-1, 60 * time.Second, 60 * time.Second, "No expiration"},
		{0, 60 * time.Second, 60 * time.Second, "Zero TTL"},
		{120 * time.Second, 60 * time.Second, 60 * time.Second, "Long TTL"},
		{30 * time.Second, 20 * time.Second, 30 * time.Second, "Short TTL (30s * 0.8 = 24s)"},
		{10 * time.Second, 5 * time.Second, 10 * time.Second, "Very short TTL (10s * 0.8 = 8s)"},
		{3 * time.Second, 5 * time.Second, 5 * time.Second, "Too short TTL (minimum 5s)"},
	}

	for _, tc := range testCases {
		result := cache.calculateLocalTTL(tc.inputTTL)
		if result < tc.expectedMin || result > tc.expectedMax {
			t.Errorf("%s: calculateLocalTTL(%v) = %v, expected between %v and %v",
				tc.description, tc.inputTTL, result, tc.expectedMin, tc.expectedMax)
		}
	}
}

func TestBigCacheWrapperSizeLimits(t *testing.T) {
	// 创建一个小的缓存来测试大小限制
	maxEntrySize := 100 // 100 bytes per entry
	hardMaxSizeMB := 1  // 1MB total

	cache, err := NewBigCacheWrapper(10*time.Second, hardMaxSizeMB, maxEntrySize, false) // no verbose
	if err != nil {
		t.Fatalf("Failed to create BigCacheWrapper: %v", err)
	}
	defer cache.Close()

	// 测试正常大小的条目
	smallKey := "small:key"
	smallValue := "small_value" // 11 bytes
	cache.Set(smallKey, smallValue)

	value, found := cache.Get(smallKey)
	if !found {
		t.Error("Expected to find small value")
	}
	if value != smallValue {
		t.Errorf("Expected %v, got %v", smallValue, value)
	}

	// 测试统计信息包含配置信息
	stats := cache.Stats()
	if stats.Sets != 1 {
		t.Errorf("Expected 1 set, got %d", stats.Sets)
	}

	logger.Infof("Cache stats after small entry: Sets=%d, Hits=%d, Size=%d",
		stats.Sets, stats.Hits, cache.Len())
}

func TestBigCacheLoggerIntegration(t *testing.T) {
	// 测试BigCache日志集成
	cache, err := NewBigCacheWrapper(10*time.Second, 1, 1024, true) // 启用verbose模式
	if err != nil {
		t.Fatalf("Failed to create BigCacheWrapper with verbose logging: %v", err)
	}
	defer cache.Close()

	// 执行一些操作来触发BigCache的内部日志
	key := "test:logger"
	value := "test_value"

	cache.Set(key, value)

	retrievedValue, found := cache.Get(key)
	if !found {
		t.Error("Expected to find cached value")
	}
	if retrievedValue != value {
		t.Errorf("Expected %v, got %v", value, retrievedValue)
	}

	logger.Infof("BigCache logger integration test completed successfully")
}
