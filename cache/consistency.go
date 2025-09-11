package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"redis-proxy-demo/config"
	"redis-proxy-demo/lib/logger"
)

// CacheEventType 缓存事件类型
type CacheEventType string

const (
	CacheInvalidate CacheEventType = "invalidate" // 缓存失效
	CacheUpdate     CacheEventType = "update"     // 缓存更新
	CacheClear      CacheEventType = "clear"      // 清空缓存
)

// CacheEvent 缓存事件消息
type CacheEvent struct {
	Type      CacheEventType `json:"type"`      // 事件类型
	Key       string         `json:"key"`       // Redis键
	Command   string         `json:"command"`   // 触发的Redis命令
	Timestamp int64          `json:"timestamp"` // 时间戳
	Source    string         `json:"source"`    // 事件源（实例ID）
	Hostname  string         `json:"hostname"`  // 机器hostname
	Database  int            `json:"database"`  // redis数据库
}

// CacheManager 缓存管理器接口
type CacheManager interface {
	InvalidateCache(key string, database int)
	Clear()
	ProcessRemoteUpdate(key string, database int)
}

// KafkaProducer Kafka生产者
type KafkaProducer struct {
	writer   *kafka.Writer
	hostname string
}

// NewKafkaProducer 创建Kafka生产者
func NewKafkaProducer(cfg *config.Config) (*KafkaProducer, error) {
	if !cfg.Kafka.Enabled {
		return nil, fmt.Errorf("kafka is not enabled")
	}

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Kafka.Brokers...),
		Topic:        cfg.Kafka.Topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}

	return &KafkaProducer{
		writer:   writer,
		hostname: hostname,
	}, nil
}

// PublishEvent 发布缓存事件
func (kp *KafkaProducer) PublishEvent(eventType CacheEventType, key string, database int, command, instanceID string) error {
	event := &CacheEvent{
		Type:      eventType,
		Key:       key,
		Command:   command,
		Timestamp: time.Now().UnixNano(),
		Source:    instanceID,
		Hostname:  kp.hostname,
		Database:  database,
	}

	// 序列化事件
	eventData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal cache event: %w", err)
	}

	// 发送到Kafka
	message := kafka.Message{
		Key:   []byte(key),
		Value: eventData,
		Time:  time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = kp.writer.WriteMessages(ctx, message)
	if err != nil {
		return fmt.Errorf("failed to publish cache event to kafka: %w", err)
	}

	logger.Debugf("Published cache event: type=%s, key=%s, command=%s", eventType, key, command)
	return nil
}

// Close 关闭生产者
func (kp *KafkaProducer) Close() error {
	if kp.writer != nil {
		return kp.writer.Close()
	}
	return nil
}

// KafkaConsumer Kafka消费者
type KafkaConsumer struct {
	reader       *kafka.Reader
	cacheManager CacheManager
	hostname     string
	instanceID   string
	stopChan     chan struct{}
	wg           sync.WaitGroup
	running      bool
	mu           sync.RWMutex
}

// NewKafkaConsumer 创建Kafka消费者
func NewKafkaConsumer(cfg *config.Config, cacheManager CacheManager, instanceID string) (*KafkaConsumer, error) {
	if !cfg.Kafka.Enabled {
		return nil, fmt.Errorf("kafka is not enabled")
	}

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}

	// 生成带hostname的消费者组ID
	consumerGroupID := fmt.Sprintf("%s-%s", cfg.Kafka.GroupID, hostname)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Kafka.Brokers,
		Topic:          cfg.Kafka.Topic,
		GroupID:        consumerGroupID,
		StartOffset:    kafka.LastOffset,
		CommitInterval: time.Second,
		MaxBytes:       10e6,
	})

	return &KafkaConsumer{
		reader:       reader,
		cacheManager: cacheManager,
		hostname:     hostname,
		instanceID:   instanceID,
		stopChan:     make(chan struct{}),
	}, nil
}

// Start 启动消费者
func (kc *KafkaConsumer) Start() error {
	kc.mu.Lock()
	defer kc.mu.Unlock()

	if kc.running {
		return fmt.Errorf("kafka consumer is already running")
	}

	kc.running = true
	kc.wg.Add(1)
	go kc.consumeMessages()

	logger.Infof("Kafka consumer started for instance: %s", kc.instanceID)
	return nil
}

// Stop 停止消费者
func (kc *KafkaConsumer) Stop() error {
	kc.mu.Lock()
	defer kc.mu.Unlock()

	if !kc.running {
		return nil
	}

	kc.running = false
	close(kc.stopChan)
	kc.wg.Wait()

	if err := kc.reader.Close(); err != nil {
		logger.Errorf("Failed to close Kafka reader: %v", err)
		return err
	}

	logger.Info("Kafka consumer stopped")
	return nil
}

// consumeMessages 消费Kafka消息
func (kc *KafkaConsumer) consumeMessages() {
	defer kc.wg.Done()

	logger.Infof("Started consuming cache events from Kafka topic: %s", kc.reader.Config().Topic)

	for {
		select {
		case <-kc.stopChan:
			logger.Info("Stopping Kafka message consumption")
			return
		default:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			message, err := kc.reader.ReadMessage(ctx)
			cancel()

			if err != nil {
				if err == context.DeadlineExceeded || err == context.Canceled {
					continue
				}
				logger.Errorf("Failed to read message from Kafka: %v", err)
				continue
			}

			// 处理消息
			if err := kc.processMessage(&message); err != nil {
				logger.Errorf("Failed to process cache event: %v", err)
			}
		}
	}
}

// processMessage 处理Kafka消息
func (kc *KafkaConsumer) processMessage(message *kafka.Message) error {
	// 反序列化事件
	var event CacheEvent
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal cache event: %w", err)
	}

	// 如果是本机发送的消息，跳过处理
	if event.Hostname == kc.hostname {
		logger.Debugf("Skipping event from same hostname: key=%s, hostname=%s", event.Key, event.Hostname)
		return nil
	}

	logger.Debugf("Processing cache event: type=%s, key=%s, command=%s, source=%s",
		event.Type, event.Key, event.Command, event.Source)

	// 处理事件
	switch event.Type {
	case CacheInvalidate:
		logger.Debugf("Invalidating cache for key: %s", event.Key)
		kc.cacheManager.InvalidateCache(event.Key, event.Database)

	case CacheUpdate:
		// 对于更新事件，直接使用消息中的value更新本地缓存
		logger.Debugf("Processing remote cache update for key: %s ", event.Key)
		kc.cacheManager.ProcessRemoteUpdate(event.Key, event.Database)

	case CacheClear:
		logger.Debug("Clearing all cache")
		kc.cacheManager.Clear()

	default:
		return fmt.Errorf("unknown cache event type: %s", event.Type)
	}

	return nil
}

// IsRunning 检查消费者是否运行中
func (kc *KafkaConsumer) IsRunning() bool {
	kc.mu.RLock()
	defer kc.mu.RUnlock()
	return kc.running
}

// GetStats 获取统计信息
func (kc *KafkaConsumer) GetStats() map[string]interface{} {
	kc.mu.RLock()
	defer kc.mu.RUnlock()

	consumerGroupID := fmt.Sprintf("%s-%s", kc.reader.Config().GroupID, kc.hostname)

	return map[string]interface{}{
		"running":           kc.running,
		"instance_id":       kc.instanceID,
		"hostname":          kc.hostname,
		"topic":             kc.reader.Config().Topic,
		"consumer_group_id": consumerGroupID,
		"brokers":           kc.reader.Config().Brokers,
	}
}

// CacheUpdateRequest 缓存更新请求
type CacheUpdateRequest struct {
	Operation string
	Key       string // Redis键
	Command   string // 触发的Redis命令
	Database  int
}

// ConsistentCache 一致性缓存
type ConsistentCache struct {
	cache      *SmartCache
	producer   *KafkaProducer
	consumer   *KafkaConsumer
	instanceID string
	enabled    bool
	mu         sync.RWMutex

	// 异步更新相关
	updateChan  chan CacheUpdateRequest
	workerCount int
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// NewConsistentCache 创建一致性缓存
func NewConsistentCache(cfg *config.Config, instanceID string) (*ConsistentCache, error) {
	// 创建智能缓存
	cache, err := NewSmartCache(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create smart cache: %w", err)
	}

	// 创建context用于优雅关闭
	ctx, cancel := context.WithCancel(context.Background())

	cc := &ConsistentCache{
		cache:       cache,
		instanceID:  instanceID,
		enabled:     cfg.Kafka.Enabled,
		updateChan:  make(chan CacheUpdateRequest, 1000), // 缓冲1000个更新请求
		workerCount: 3,                                   // 启动3个worker goroutine
		ctx:         ctx,
		cancel:      cancel,
	}

	// 如果Kafka启用，创建生产者和消费者
	if cfg.Kafka.Enabled {
		producer, err := NewKafkaProducer(cfg)
		if err != nil {
			logger.Errorf("Failed to create Kafka producer: %v", err)
			// 不返回错误，继续使用本地缓存
		} else {
			cc.producer = producer
		}

		consumer, err := NewKafkaConsumer(cfg, cache, instanceID)
		if err != nil {
			logger.Errorf("Failed to create Kafka consumer: %v", err)
			// 不返回错误，继续使用本地缓存
		} else {
			cc.consumer = consumer
			// 启动消费者
			if err := cc.consumer.Start(); err != nil {
				logger.Errorf("Failed to start Kafka consumer: %v", err)
			}
		}
	}

	// 启动异步缓存更新worker goroutines
	cc.startAsyncWorkers()

	logger.Infof("✅ 异步缓存更新已启用 (Workers: %d, Buffer: %d)", cc.workerCount, cap(cc.updateChan))

	return cc, nil
}

// startAsyncWorkers 启动异步缓存更新worker goroutines
func (cc *ConsistentCache) startAsyncWorkers() {
	for i := 0; i < cc.workerCount; i++ {
		cc.wg.Add(1)
		go cc.asyncCacheWorker(i)
	}
	logger.Debugf("Started %d async cache workers", cc.workerCount)
}

// asyncCacheWorker 异步缓存更新worker
func (cc *ConsistentCache) asyncCacheWorker(workerID int) {
	defer cc.wg.Done()

	logger.Debugf("Async cache worker %d started", workerID)

	for {
		select {
		case <-cc.ctx.Done():
			logger.Debugf("Async cache worker %d shutting down", workerID)
			return

		case req := <-cc.updateChan:
			cc.processAsyncCacheUpdate(workerID, req)
		}
	}
}

// processAsyncCacheUpdate 处理异步缓存更新
func (cc *ConsistentCache) processAsyncCacheUpdate(workerID int, req CacheUpdateRequest) {
	start := time.Now()

	switch req.Operation {
	case "SET":
		// 更新本地缓存
		cc.cache.ProcessSET(req.Key, req.Database)

		// 发布Kafka事件
		if cc.enabled && cc.producer != nil {
			if err := cc.producer.PublishEvent(CacheUpdate, req.Key, req.Database, req.Command, cc.instanceID); err != nil {
				logger.Errorf("Worker %d failed to publish cache update event: %v", workerID, err)
			}
		}

	case "INVALIDATE":
		// 本地缓存失效
		cc.cache.InvalidateCache(req.Key, req.Database)

		// 广播失效事件
		if cc.enabled && cc.producer != nil {
			if err := cc.producer.PublishEvent(CacheInvalidate, req.Key, req.Database, "INVALIDATE", cc.instanceID); err != nil {
				logger.Errorf("Worker %d failed to publish cache invalidate event: %v", workerID, err)
			}
		}

	case "CLEAR":
		// 本地缓存清空
		cc.cache.Clear()

		// 广播清空事件
		if cc.enabled && cc.producer != nil {
			if err := cc.producer.PublishEvent(CacheClear, "", req.Database, "FLUSHALL", cc.instanceID); err != nil {
				logger.Errorf("Worker %d failed to publish cache clear event: %v", workerID, err)
			}
		}

	default:
		logger.Errorf("Worker %d received unknown cache operation: %s", workerID, req.Operation)
		return
	}

	duration := time.Since(start)
	logger.Debugf("Worker %d processed %s operation for key '%s' in %v",
		workerID, req.Operation, req.Key, duration)
}

// ProcessGET 处理GET命令
func (cc *ConsistentCache) ProcessGET(key string, database int) (interface{}, bool) {
	return cc.cache.ProcessGET(key, database)
}

// ProcessSET 处理SET命令并异步广播
func (cc *ConsistentCache) ProcessSET(key string, command string, database int) {
	// 检查key是否应该被缓存
	if !cc.cache.ShouldCacheKey(key) {
		logger.Debugf("Key %s matches no-cache prefix, skipping SET processing", key)
		return
	}

	operation := "SET"
	// 异步提交缓存更新请求
	select {
	case cc.updateChan <- CacheUpdateRequest{
		Operation: operation,
		Key:       key,
		Command:   command,
		Database:  database,
	}:
		// 成功提交到异步队列
		logger.Debugf("Async SET request queued for key: %s", key)
	default:
		// 队列满了，同步处理以避免丢失
		logger.Warn(fmt.Sprintf("Async queue full, processing SET synchronously for key: %s", key))
		cc.cache.ProcessSET(key, database)
		if cc.enabled && cc.producer != nil {
			if err := cc.producer.PublishEvent(CacheUpdate, key, database, "SET", cc.instanceID); err != nil {
				logger.Errorf("Failed to publish cache update event: %v", err)
			}
		}
	}
}

// InvalidateCache 使缓存失效并异步广播
func (cc *ConsistentCache) InvalidateCache(key string, database int) {
	// 检查key是否应该被缓存
	if !cc.cache.ShouldCacheKey(key) {
		logger.Debugf("Key %s matches no-cache prefix, skipping INVALIDATE processing", key)
		return
	}

	// 异步提交缓存失效请求
	select {
	case cc.updateChan <- CacheUpdateRequest{
		Key:       key,
		Command:   "DEL",
		Operation: "INVALIDATE",
		Database:  database,
	}:
		// 成功提交到异步队列
		logger.Debugf("Async INVALIDATE request queued for key: %s database:%d", key, database)
	default:
		// 队列满了，同步处理以避免丢失
		logger.Warn(fmt.Sprintf("Async queue full, processing INVALIDATE synchronously for key: %s", key))
		cc.cache.InvalidateCache(key, database)
		if cc.enabled && cc.producer != nil {
			if err := cc.producer.PublishEvent(CacheInvalidate, key, database, "INVALIDATE", cc.instanceID); err != nil {
				logger.Errorf("Failed to publish cache invalidate event: %v", err)
			}
		}
	}
}

// Clear 清空缓存并异步广播
func (cc *ConsistentCache) Clear() {
	// 异步提交缓存清空请求
	select {
	case cc.updateChan <- CacheUpdateRequest{
		Command:   "FLUSHALL",
		Operation: "CLEAR",
	}:
		// 成功提交到异步队列
		logger.Debugf("Async CLEAR request queued")
	default:
		// 队列满了，同步处理以避免丢失
		logger.Warn("Async queue full, processing CLEAR synchronously")
		cc.cache.Clear()
		if cc.enabled && cc.producer != nil {
			if err := cc.producer.PublishEvent(CacheClear, "", 0, "FLUSHALL", cc.instanceID); err != nil {
				logger.Errorf("Failed to publish cache clear event: %v", err)
			}
		}
	}
}

// GetStats 获取统计信息
func (cc *ConsistentCache) GetStats() map[string]interface{} {
	cc.mu.RLock()
	defer cc.mu.RUnlock()

	stats := cc.cache.GetStats()
	stats["instance_id"] = cc.instanceID
	stats["kafka_enabled"] = cc.enabled

	if cc.consumer != nil {
		consumerStats := cc.consumer.GetStats()
		for k, v := range consumerStats {
			stats["kafka_"+k] = v
		}
	}

	return stats
}

// Close 关闭一致性缓存
func (cc *ConsistentCache) Close() error {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	var errs []error

	// 1. 停止接收新的异步请求
	logger.Info("🔄 停止异步缓存更新...")
	if cc.cancel != nil {
		cc.cancel()
	}

	// 2. 等待所有worker完成当前工作
	logger.Info("⏳ 等待异步worker完成...")
	done := make(chan struct{})
	go func() {
		cc.wg.Wait()
		close(done)
	}()

	// 等待最多5秒让worker完成
	select {
	case <-done:
		logger.Info("✅ 所有异步worker已完成")
	case <-time.After(5 * time.Second):
		logger.Warn("⚠️ 异步worker超时，强制关闭")
	}

	// 3. 处理剩余的队列中的请求
	close(cc.updateChan)
	remainingCount := 0
	for req := range cc.updateChan {
		cc.processAsyncCacheUpdate(-1, req) // 使用-1表示关闭时的处理
		remainingCount++
	}
	if remainingCount > 0 {
		logger.Infof("🧹 处理了 %d 个剩余的异步请求", remainingCount)
	}

	// 4. 停止Kafka消费者
	if cc.consumer != nil {
		if err := cc.consumer.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("failed to stop consumer: %w", err))
		}
	}

	// 5. 关闭Kafka生产者
	if cc.producer != nil {
		if err := cc.producer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close producer: %w", err))
		}
	}

	// 6. 关闭本地缓存
	if cc.cache != nil {
		if err := cc.cache.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close cache: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("multiple errors during close: %v", errs)
	}

	logger.Info("✅ 异步缓存系统已完全关闭")
	return nil
}
