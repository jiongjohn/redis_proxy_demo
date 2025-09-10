package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/allegro/bigcache/v3"

	"redis-proxy-demo/config"
	"redis-proxy-demo/lib/logger"
)

// BigCacheLoggerAdapter 适配器，将我们的logger适配到BigCache的Logger接口
type BigCacheLoggerAdapter struct {
	logger logger.ILogger
}

// NewBigCacheLoggerAdapter 创建BigCache日志适配器
func NewBigCacheLoggerAdapter(l logger.ILogger) *BigCacheLoggerAdapter {
	return &BigCacheLoggerAdapter{logger: l}
}

// Printf 实现BigCache的Logger接口
func (adapter *BigCacheLoggerAdapter) Printf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	// BigCache的日志通常是调试信息，使用DEBUG级别
	adapter.logger.Output(logger.DEBUG, 3, msg) // callerDepth=3 跳过适配器层
}

// CacheEntry 缓存条目
type CacheEntry struct {
	Value     interface{} `json:"value"`
	ExpiresAt int64       `json:"expires_at"` // Unix timestamp
	CreatedAt int64       `json:"created_at"` // Unix timestamp
}

// BigCacheWrapper BigCache 封装器
type BigCacheWrapper struct {
	cache      *bigcache.BigCache
	stats      *CacheStats
	enabled    bool
	defaultTTL time.Duration
}

// CacheStats 缓存统计信息
type CacheStats struct {
	Hits      int64   `json:"hits"`
	Misses    int64   `json:"misses"`
	Sets      int64   `json:"sets"`
	Deletes   int64   `json:"deletes"`
	Evictions int64   `json:"evictions"`
	HitRate   float64 `json:"hit_rate"`
}

// NewBigCacheWrapper 创建新的 BigCache 封装器
func NewBigCacheWrapper(defaultTTL time.Duration, hardMaxCacheSize int, maxEntrySize int, verbose bool) (*BigCacheWrapper, error) {
	// 根据硬限制大小计算合理的MaxEntriesInWindow
	// 假设平均每个条目约1KB，预估条目数量
	estimatedEntries := hardMaxCacheSize * 1024 // MB转换为KB数量级的条目数
	if estimatedEntries <= 0 {
		estimatedEntries = 10000 // 默认值
	}

	config := bigcache.Config{
		Shards:             1024,                                           // 分片数量
		LifeWindow:         defaultTTL,                                     // 条目生存时间
		CleanWindow:        time.Minute,                                    // 清理窗口
		MaxEntriesInWindow: estimatedEntries,                               // 窗口内最大条目数（自动计算）
		MaxEntrySize:       maxEntrySize,                                   // 最大条目大小（可配置）
		HardMaxCacheSize:   hardMaxCacheSize,                               // 硬限制缓存大小（可配置）
		Verbose:            verbose,                                        // 启用详细日志（可配置）
		OnRemove:           nil,                                            // 删除回调
		OnRemoveWithReason: nil,                                            // 带原因的删除回调
		Logger:             NewBigCacheLoggerAdapter(logger.DefaultLogger), // 使用我们的自定义日志记录器
	}

	cache, err := bigcache.New(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create bigcache: %w", err)
	}

	return &BigCacheWrapper{
		cache:      cache,
		stats:      &CacheStats{},
		enabled:    true,
		defaultTTL: defaultTTL,
	}, nil
}

// Get 获取缓存值
func (bcw *BigCacheWrapper) Get(key string) (interface{}, bool) {
	if !bcw.enabled {
		return nil, false
	}

	data, err := bcw.cache.Get(key)
	if err != nil {
		atomic.AddInt64(&bcw.stats.Misses, 1)
		bcw.updateHitRate()
		return nil, false
	}

	// 反序列化条目
	var entry CacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		logger.Errorf("Failed to unmarshal cache entry for key %s: %v", key, err)
		atomic.AddInt64(&bcw.stats.Misses, 1)
		bcw.updateHitRate()
		return nil, false
	}

	atomic.AddInt64(&bcw.stats.Hits, 1)
	bcw.updateHitRate()
	return entry.Value, true
}

// Set 设置缓存值
func (bcw *BigCacheWrapper) Set(key string, value interface{}) {
	bcw.SetWithTTL(key, value, bcw.defaultTTL)
}

// SetWithTTL 设置缓存值和TTL
func (bcw *BigCacheWrapper) SetWithTTL(key string, value interface{}, ttl time.Duration) {
	if !bcw.enabled {
		return
	}

	var expiresAt int64
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl).Unix()
	}

	entry := CacheEntry{
		Value:     value,
		ExpiresAt: expiresAt,
		CreatedAt: time.Now().Unix(),
	}

	data, err := json.Marshal(entry)
	if err != nil {
		logger.Errorf("Failed to marshal cache entry for key %s: %v", key, err)
		return
	}

	err = bcw.cache.Set(key, data)
	if err != nil {
		logger.Errorf("Failed to set cache entry for key %s: %v", key, err)
		return
	}

	atomic.AddInt64(&bcw.stats.Sets, 1)
}

// Delete 删除缓存值
func (bcw *BigCacheWrapper) Delete(key string) {
	if !bcw.enabled {
		return
	}

	err := bcw.cache.Delete(key)
	if err != nil {
		logger.Debugf("Failed to delete cache key %s: %v", key, err)
		return
	}

	atomic.AddInt64(&bcw.stats.Deletes, 1)
}

// Clear 清空缓存
func (bcw *BigCacheWrapper) Clear() {
	if !bcw.enabled {
		return
	}

	err := bcw.cache.Reset()
	if err != nil {
		logger.Errorf("Failed to reset cache: %v", err)
		return
	}

	// 重置统计信息
	atomic.StoreInt64(&bcw.stats.Hits, 0)
	atomic.StoreInt64(&bcw.stats.Misses, 0)
	atomic.StoreInt64(&bcw.stats.Sets, 0)
	atomic.StoreInt64(&bcw.stats.Deletes, 0)
	atomic.StoreInt64(&bcw.stats.Evictions, 0)
	bcw.stats.HitRate = 0
}

// Len 获取缓存条目数量
func (bcw *BigCacheWrapper) Len() int {
	if !bcw.enabled {
		return 0
	}

	return bcw.cache.Len()
}

// Stats 获取缓存统计信息
func (bcw *BigCacheWrapper) Stats() CacheStats {
	return CacheStats{
		Hits:      atomic.LoadInt64(&bcw.stats.Hits),
		Misses:    atomic.LoadInt64(&bcw.stats.Misses),
		Sets:      atomic.LoadInt64(&bcw.stats.Sets),
		Deletes:   atomic.LoadInt64(&bcw.stats.Deletes),
		Evictions: atomic.LoadInt64(&bcw.stats.Evictions),
		HitRate:   bcw.stats.HitRate,
	}
}

// updateHitRate 更新命中率
func (bcw *BigCacheWrapper) updateHitRate() {
	hits := atomic.LoadInt64(&bcw.stats.Hits)
	misses := atomic.LoadInt64(&bcw.stats.Misses)
	total := hits + misses
	if total > 0 {
		bcw.stats.HitRate = float64(hits) / float64(total)
	}
}

// Close 关闭缓存
func (bcw *BigCacheWrapper) Close() error {
	if bcw.cache != nil {
		return bcw.cache.Close()
	}
	return nil
}

// SmartCache 智能缓存系统
type SmartCache struct {
	localCache *BigCacheWrapper
	config     *config.Config
	enabled    bool
	hostname   string
	mu         sync.RWMutex
}

// NewSmartCache 创建新的智能缓存
func NewSmartCache(cfg *config.Config) (*SmartCache, error) {
	if cfg == nil || !cfg.Cache.Enabled {
		return &SmartCache{
			enabled: false,
		}, nil
	}

	// 获取hostname
	hostname, err := os.Hostname()
	if err != nil {
		logger.Errorf("Failed to get hostname: %v", err)
		hostname = "unknown"
	}

	// 解析TTL
	ttl, err := time.ParseDuration(cfg.Cache.TTL)
	if err != nil {
		return nil, fmt.Errorf("invalid cache TTL: %w", err)
	}

	// 创建BigCache
	localCache, err := NewBigCacheWrapper(
		ttl,
		cfg.Cache.HardMaxCacheSize,
		cfg.Cache.MaxEntrySize,
		cfg.Cache.Verbose,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create local cache: %w", err)
	}

	return &SmartCache{
		localCache: localCache,
		config:     cfg,
		enabled:    true,
		hostname:   hostname,
	}, nil
}

// IsEnabled 检查缓存是否启用
func (sc *SmartCache) IsEnabled() bool {
	return sc.enabled
}

// GetHostname 获取hostname
func (sc *SmartCache) GetHostname() string {
	return sc.hostname
}

// ShouldCache 检查命令是否应该缓存
func (sc *SmartCache) ShouldCache(command string) bool {
	if !sc.enabled {
		return false
	}
	// 只处理 GET 和 SET 命令
	cmd := strings.ToUpper(strings.TrimSpace(command))
	return cmd == "GET" || cmd == "SET"
}

// ProcessGET 处理GET命令
func (sc *SmartCache) ProcessGET(key string) (interface{}, bool) {
	if !sc.ShouldCache("GET") {
		return nil, false
	}

	sc.mu.RLock()
	defer sc.mu.RUnlock()

	// 检查本地缓存
	cacheKey := fmt.Sprintf("GET:%s", key)
	if value, found := sc.localCache.Get(cacheKey); found {
		logger.Debugf("Cache HIT for key: %s", key)
		return value, true
	}

	logger.Debugf("Cache MISS for key: %s", key)
	return nil, false
}

// ProcessSET 处理SET命令
func (sc *SmartCache) ProcessSET(key string, value interface{}, ttl time.Duration) {
	if !sc.ShouldCache("SET") {
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	// 计算本地缓存TTL
	localTTL := sc.calculateLocalTTL(ttl)

	// 更新本地缓存
	cacheKey := fmt.Sprintf("GET:%s", key) // SET命令影响GET的缓存
	sc.localCache.SetWithTTL(cacheKey, value, localTTL)

	logger.Debugf("Updated local cache for SET command, key: %s, TTL: %v", key, localTTL)
}

// calculateLocalTTL 计算本地缓存TTL
func (sc *SmartCache) calculateLocalTTL(redisTTL time.Duration) time.Duration {
	defaultTTL := 60 * time.Second

	// 如果Redis没有设置TTL或TTL很长，使用默认60秒
	if redisTTL <= 0 || redisTTL >= defaultTTL {
		return defaultTTL
	}

	// 如果Redis TTL小于60秒，使用 redisTTL * 0.8
	localTTL := time.Duration(float64(redisTTL) * 0.8)

	// 确保至少有5秒的缓存时间
	if localTTL < 5*time.Second {
		localTTL = 5 * time.Second
	}

	return localTTL
}

// ProcessRemoteUpdate 处理远程更新，直接使用传入的value更新本地缓存
func (sc *SmartCache) ProcessRemoteUpdate(key string, value interface{}) {
	if !sc.enabled {
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	logger.Debugf("Processing remote update for key: %s", key)

	// 直接更新本地缓存，使用默认TTL
	cacheKey := fmt.Sprintf("GET:%s", key)
	sc.localCache.Set(cacheKey, value)

	logger.Debugf("Updated local cache for remote update, key: %s", key)
}

// InvalidateCache 使缓存失效
func (sc *SmartCache) InvalidateCache(key string) {
	if !sc.enabled {
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	cacheKey := fmt.Sprintf("GET:%s", key)
	sc.localCache.Delete(cacheKey)
	logger.Debugf("Invalidated cache for key: %s", key)
}

// SetToCache 设置缓存 (实现CacheManager接口)
func (sc *SmartCache) SetToCache(command, key string, value interface{}) {
	if !sc.enabled {
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	cacheKey := fmt.Sprintf("%s:%s", command, key)
	sc.localCache.Set(cacheKey, value)
	logger.Debugf("Set cache: command=%s, key=%s, value type=%T", command, key, value)
}

// Clear 清空所有缓存
func (sc *SmartCache) Clear() {
	if !sc.enabled {
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.localCache.Clear()
	logger.Debug("Cleared all local cache")
}

// GetStats 获取缓存统计信息
func (sc *SmartCache) GetStats() map[string]interface{} {
	if !sc.enabled {
		return map[string]interface{}{
			"enabled": false,
		}
	}

	sc.mu.RLock()
	defer sc.mu.RUnlock()

	stats := sc.localCache.Stats()
	return map[string]interface{}{
		"enabled":   true,
		"hostname":  sc.hostname,
		"hits":      stats.Hits,
		"misses":    stats.Misses,
		"sets":      stats.Sets,
		"deletes":   stats.Deletes,
		"size":      sc.localCache.Len(),
		"hit_rate":  stats.HitRate,
		"evictions": stats.Evictions,
	}
}

// Close 关闭缓存集成器
func (sc *SmartCache) Close() error {
	if sc.localCache != nil {
		return sc.localCache.Close()
	}
	return nil
}
