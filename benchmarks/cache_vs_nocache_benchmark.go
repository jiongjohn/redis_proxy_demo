package main

import (
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	fmt.Println("🔥 Redis Proxy 缓存 vs 非缓存性能对比测试")
	fmt.Println("==========================================")

	// 测试参数
	const (
		host           = "127.0.0.1:6380"
		numClients     = 50              // 并发客户端数
		totalRequests  = 100000          // 总请求数 (每组)
		numKeys        = 1000            // 每组的key数量
		reportInterval = 2 * time.Second // 报告间隔
	)

	fmt.Printf("测试配置:\n")
	fmt.Printf("├── 目标服务器: %s\n", host)
	fmt.Printf("├── 并发客户端: %d\n", numClients)
	fmt.Printf("├── 每组总请求数: %d\n", totalRequests)
	fmt.Printf("├── 缓存Key数量: %d\n", numKeys)
	fmt.Printf("├── 非缓存Key数量: %d (nocache:前缀)\n", numKeys)
	fmt.Printf("└── 报告间隔: %v\n", reportInterval)
	fmt.Println()

	// 预热数据
	fmt.Println("🔧 预热数据...")
	if err := warmupCacheVsNoCacheData(host, numKeys); err != nil {
		fmt.Printf("❌ 预热数据失败: %v\n", err)
		return
	}
	fmt.Println("✅ 数据预热完成")
	fmt.Println()

	// 等待缓存稳定
	fmt.Println("⏳ 等待缓存稳定...")
	time.Sleep(3 * time.Second)

	// 测试缓存Key
	fmt.Println("🚀 开始测试缓存Key性能...")
	cacheStats := runCacheVsNoCacheBenchmark(host, numClients, totalRequests, numKeys, true, reportInterval)

	fmt.Println("\n⏳ 等待系统稳定...")
	time.Sleep(5 * time.Second)

	// 测试非缓存Key
	fmt.Println("🚀 开始测试非缓存Key性能...")
	noCacheStats := runCacheVsNoCacheBenchmark(host, numClients, totalRequests, numKeys, false, reportInterval)

	// 打印对比结果
	printComparisonResults(cacheStats, noCacheStats)
}

// CacheVsNoCacheStats 性能统计
type CacheVsNoCacheStats struct {
	TestType      string
	TotalRequests int64
	TotalErrors   int64
	Duration      time.Duration

	// 延迟统计 (纳秒)
	TotalLatency int64
	MinLatency   int64
	MaxLatency   int64

	// 延迟分布
	Under100us int64 // < 100微秒
	Under200us int64 // < 200微秒
	Under500us int64 // < 500微秒
	Under1ms   int64 // < 1毫秒
	Under2ms   int64 // < 2毫秒
	Under5ms   int64 // < 5毫秒
	Over5ms    int64 // > 5毫秒
}

// warmupCacheVsNoCacheData 预热缓存和非缓存数据
func warmupCacheVsNoCacheData(host string, numKeys int) error {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		return fmt.Errorf("连接失败: %w", err)
	}
	defer conn.Close()

	// 设置缓存Key数据
	fmt.Printf("   设置缓存Key数据 (cache_key_*)...\n")
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("cache_key_%d", i)
		value := fmt.Sprintf("cache_value_%d_%d", i, time.Now().Unix())
		if err := sendSetCommand(conn, key, value); err != nil {
			return fmt.Errorf("设置缓存Key失败: %w", err)
		}
		if i%100 == 0 {
			fmt.Printf("     已设置缓存Key: %d/%d\n", i+1, numKeys)
		}
	}

	// 设置非缓存Key数据 (nocache:前缀)
	fmt.Printf("   设置非缓存Key数据 (nocache:nocache_key_*)...\n")
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("nocache:nocache_key_%d", i)
		value := fmt.Sprintf("nocache_value_%d_%d", i, time.Now().Unix())
		if err := sendSetCommand(conn, key, value); err != nil {
			return fmt.Errorf("设置非缓存Key失败: %w", err)
		}
		if i%100 == 0 {
			fmt.Printf("     已设置非缓存Key: %d/%d\n", i+1, numKeys)
		}
	}

	// 预热缓存Key (多次GET确保缓存)
	fmt.Printf("   预热缓存Key (执行GET操作)...\n")
	for round := 0; round < 5; round++ {
		for i := 0; i < numKeys; i += 10 { // 每10个key取一个进行预热
			key := fmt.Sprintf("cache_key_%d", i)
			if err := sendGetCommand(conn, key); err != nil {
				return fmt.Errorf("预热缓存Key失败: %w", err)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

// runCacheVsNoCacheBenchmark 运行基准测试
func runCacheVsNoCacheBenchmark(host string, numClients, totalRequests, numKeys int, useCache bool, reportInterval time.Duration) *CacheVsNoCacheStats {
	stats := &CacheVsNoCacheStats{}
	if useCache {
		stats.TestType = "缓存Key测试"
	} else {
		stats.TestType = "非缓存Key测试"
	}

	// 启动统计报告协程
	stopReporting := make(chan bool)
	go reportCacheVsNoCacheStats(stats, reportInterval, stopReporting)

	startTime := time.Now()
	var wg sync.WaitGroup
	stopTesting := make(chan bool)

	// 计算每个客户端的请求数
	requestsPerClient := totalRequests / numClients

	// 启动客户端协程
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			runCacheVsNoCacheClient(clientID, host, stats, stopTesting, requestsPerClient, numKeys, useCache)
		}(i)
	}

	// 等待所有请求完成
	wg.Wait()
	close(stopTesting)
	close(stopReporting)

	stats.Duration = time.Since(startTime)
	return stats
}

// runCacheVsNoCacheClient 运行客户端测试
func runCacheVsNoCacheClient(clientID int, host string, stats *CacheVsNoCacheStats, stopTesting <-chan bool,
	requestsPerClient, numKeys int, useCache bool) {

	// 建立连接
	conn, err := net.Dial("tcp", host)
	if err != nil {
		atomic.AddInt64(&stats.TotalErrors, 1)
		return
	}
	defer conn.Close()

	// 设置连接超时
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	conn.SetWriteDeadline(time.Now().Add(30 * time.Second))

	rand.Seed(time.Now().UnixNano() + int64(clientID))

	// 执行指定数量的请求
	for i := 0; i < requestsPerClient; i++ {
		select {
		case <-stopTesting:
			return
		default:
			// 随机选择一个key
			var key string
			keyIndex := rand.Intn(numKeys)

			if useCache {
				key = fmt.Sprintf("cache_key_%d", keyIndex)
			} else {
				key = fmt.Sprintf("nocache:nocache_key_%d", keyIndex)
			}

			// 执行GET操作
			executeCacheVsNoCacheGet(conn, key, stats)

			// 重置超时
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		}
	}
}

// executeCacheVsNoCacheGet 执行GET操作
func executeCacheVsNoCacheGet(conn net.Conn, key string, stats *CacheVsNoCacheStats) {
	start := time.Now()
	err := sendGetCommand(conn, key)
	latency := time.Since(start).Nanoseconds()

	atomic.AddInt64(&stats.TotalRequests, 1)

	if err != nil {
		atomic.AddInt64(&stats.TotalErrors, 1)
		return
	}

	// 更新延迟统计
	atomic.AddInt64(&stats.TotalLatency, latency)
	updateCacheVsNoCacheLatencyStats(stats, latency)
}

// updateCacheVsNoCacheLatencyStats 更新延迟统计
func updateCacheVsNoCacheLatencyStats(stats *CacheVsNoCacheStats, latency int64) {
	latencyUs := float64(latency) / 1000 // 转换为微秒

	if latencyUs < 100 {
		atomic.AddInt64(&stats.Under100us, 1)
	} else if latencyUs < 200 {
		atomic.AddInt64(&stats.Under200us, 1)
	} else if latencyUs < 500 {
		atomic.AddInt64(&stats.Under500us, 1)
	} else if latencyUs < 1000 {
		atomic.AddInt64(&stats.Under1ms, 1)
	} else if latencyUs < 2000 {
		atomic.AddInt64(&stats.Under2ms, 1)
	} else if latencyUs < 5000 {
		atomic.AddInt64(&stats.Under5ms, 1)
	} else {
		atomic.AddInt64(&stats.Over5ms, 1)
	}

	// 更新最小/最大延迟
	for {
		current := atomic.LoadInt64(&stats.MinLatency)
		if current != 0 && latency >= current {
			break
		}
		if atomic.CompareAndSwapInt64(&stats.MinLatency, current, latency) {
			break
		}
	}

	for {
		current := atomic.LoadInt64(&stats.MaxLatency)
		if latency <= current {
			break
		}
		if atomic.CompareAndSwapInt64(&stats.MaxLatency, current, latency) {
			break
		}
	}
}

// reportCacheVsNoCacheStats 定期报告统计信息
func reportCacheVsNoCacheStats(stats *CacheVsNoCacheStats, interval time.Duration, stop <-chan bool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			printCurrentCacheVsNoCacheStats(stats)
		}
	}
}

// printCurrentCacheVsNoCacheStats 打印当前统计信息
func printCurrentCacheVsNoCacheStats(stats *CacheVsNoCacheStats) {
	totalReqs := atomic.LoadInt64(&stats.TotalRequests)
	totalErrors := atomic.LoadInt64(&stats.TotalErrors)

	if totalReqs == 0 {
		return
	}

	fmt.Printf("\n📊 %s 实时统计 [%s]\n", stats.TestType, time.Now().Format("15:04:05"))
	fmt.Printf("├── 已完成请求: %d (错误: %d, 成功率: %.2f%%)\n",
		totalReqs, totalErrors, float64(totalReqs-totalErrors)/float64(totalReqs)*100)

	// 计算平均延迟
	if totalReqs > 0 {
		avgLatency := float64(atomic.LoadInt64(&stats.TotalLatency)) / float64(totalReqs) / 1000000
		minLatency := float64(atomic.LoadInt64(&stats.MinLatency)) / 1000000
		maxLatency := float64(atomic.LoadInt64(&stats.MaxLatency)) / 1000000
		fmt.Printf("├── 平均延迟: %.3f ms (最小: %.3f ms, 最大: %.3f ms)\n",
			avgLatency, minLatency, maxLatency)
	}

	// 显示延迟分布
	under100us := atomic.LoadInt64(&stats.Under100us)
	under200us := atomic.LoadInt64(&stats.Under200us)
	under500us := atomic.LoadInt64(&stats.Under500us)
	under1ms := atomic.LoadInt64(&stats.Under1ms)

	fmt.Printf("└── 延迟分布: <100μs: %.1f%%, <200μs: %.1f%%, <500μs: %.1f%%, <1ms: %.1f%%\n",
		float64(under100us)/float64(totalReqs)*100,
		float64(under200us)/float64(totalReqs)*100,
		float64(under500us)/float64(totalReqs)*100,
		float64(under1ms)/float64(totalReqs)*100)
}

// printComparisonResults 打印对比结果
func printComparisonResults(cacheStats, noCacheStats *CacheVsNoCacheStats) {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("🏁 缓存 vs 非缓存性能对比最终结果")
	fmt.Println(strings.Repeat("=", 80))

	// 基础统计对比
	fmt.Printf("📈 请求统计对比:\n")
	fmt.Printf("├── 缓存Key请求: %d (错误: %d, 成功率: %.3f%%)\n",
		cacheStats.TotalRequests, cacheStats.TotalErrors,
		float64(cacheStats.TotalRequests-cacheStats.TotalErrors)/float64(cacheStats.TotalRequests)*100)
	fmt.Printf("└── 非缓存Key请求: %d (错误: %d, 成功率: %.3f%%)\n",
		noCacheStats.TotalRequests, noCacheStats.TotalErrors,
		float64(noCacheStats.TotalRequests-noCacheStats.TotalErrors)/float64(noCacheStats.TotalRequests)*100)

	// 性能统计对比
	cacheQPS := float64(cacheStats.TotalRequests) / cacheStats.Duration.Seconds()
	noCacheQPS := float64(noCacheStats.TotalRequests) / noCacheStats.Duration.Seconds()

	fmt.Printf("\n⚡ 性能统计对比:\n")
	fmt.Printf("├── 缓存Key QPS: %.2f req/s (测试时长: %v)\n", cacheQPS, cacheStats.Duration)
	fmt.Printf("└── 非缓存Key QPS: %.2f req/s (测试时长: %v)\n", noCacheQPS, noCacheStats.Duration)

	// 延迟统计对比
	if cacheStats.TotalRequests > 0 && noCacheStats.TotalRequests > 0 {
		cacheAvgLatency := float64(cacheStats.TotalLatency) / float64(cacheStats.TotalRequests) / 1000000
		cacheMinLatency := float64(cacheStats.MinLatency) / 1000000
		cacheMaxLatency := float64(cacheStats.MaxLatency) / 1000000

		noCacheAvgLatency := float64(noCacheStats.TotalLatency) / float64(noCacheStats.TotalRequests) / 1000000
		noCacheMinLatency := float64(noCacheStats.MinLatency) / 1000000
		noCacheMaxLatency := float64(noCacheStats.MaxLatency) / 1000000

		fmt.Printf("\n🕐 延迟统计对比:\n")
		fmt.Printf("├── 缓存Key延迟: 平均 %.3f ms (最小: %.3f ms, 最大: %.3f ms)\n",
			cacheAvgLatency, cacheMinLatency, cacheMaxLatency)
		fmt.Printf("└── 非缓存Key延迟: 平均 %.3f ms (最小: %.3f ms, 最大: %.3f ms)\n",
			noCacheAvgLatency, noCacheMinLatency, noCacheMaxLatency)

		// 延迟分布对比
		fmt.Printf("\n📊 延迟分布对比:\n")
		fmt.Printf("缓存Key延迟分布:\n")
		printLatencyDistribution(cacheStats)
		fmt.Printf("非缓存Key延迟分布:\n")
		printLatencyDistribution(noCacheStats)

		// 性能提升分析
		fmt.Printf("\n🎯 性能对比分析:\n")

		qpsImprovement := (cacheQPS - noCacheQPS) / noCacheQPS * 100
		latencyImprovement := (noCacheAvgLatency - cacheAvgLatency) / noCacheAvgLatency * 100

		if qpsImprovement > 0 {
			fmt.Printf("├── QPS提升: 缓存比非缓存高 %.1f%%\n", qpsImprovement)
		} else {
			fmt.Printf("├── QPS对比: 非缓存比缓存高 %.1f%%\n", -qpsImprovement)
		}

		if latencyImprovement > 0 {
			fmt.Printf("├── 延迟优化: 缓存比非缓存快 %.1f%%\n", latencyImprovement)
		} else {
			fmt.Printf("├── 延迟对比: 非缓存比缓存快 %.1f%%\n", -latencyImprovement)
		}

		// 超快响应率对比
		cacheUltraFast := float64(cacheStats.Under100us+cacheStats.Under200us+cacheStats.Under500us) / float64(cacheStats.TotalRequests) * 100
		noCacheUltraFast := float64(noCacheStats.Under100us+noCacheStats.Under200us+noCacheStats.Under500us) / float64(noCacheStats.TotalRequests) * 100

		fmt.Printf("├── 缓存Key超快响应率(<500μs): %.1f%%\n", cacheUltraFast)
		fmt.Printf("├── 非缓存Key超快响应率(<500μs): %.1f%%\n", noCacheUltraFast)

		if cacheUltraFast > noCacheUltraFast {
			fmt.Printf("└── 🎉 缓存效果: 缓存超快响应率高出 %.1f 个百分点\n", cacheUltraFast-noCacheUltraFast)
		} else {
			fmt.Printf("└── ⚠️ 缓存效果: 非缓存超快响应率高出 %.1f 个百分点\n", noCacheUltraFast-cacheUltraFast)
		}
	}

	fmt.Println(strings.Repeat("=", 80))
}

// printLatencyDistribution 打印延迟分布
func printLatencyDistribution(stats *CacheVsNoCacheStats) {
	total := float64(stats.TotalRequests)
	fmt.Printf("├── < 100μs: %d (%.2f%%)\n", stats.Under100us, float64(stats.Under100us)/total*100)
	fmt.Printf("├── < 200μs: %d (%.2f%%)\n", stats.Under200us, float64(stats.Under200us)/total*100)
	fmt.Printf("├── < 500μs: %d (%.2f%%)\n", stats.Under500us, float64(stats.Under500us)/total*100)
	fmt.Printf("├── < 1ms: %d (%.2f%%)\n", stats.Under1ms, float64(stats.Under1ms)/total*100)
	fmt.Printf("├── < 2ms: %d (%.2f%%)\n", stats.Under2ms, float64(stats.Under2ms)/total*100)
	fmt.Printf("├── < 5ms: %d (%.2f%%)\n", stats.Under5ms, float64(stats.Under5ms)/total*100)
	fmt.Printf("└── > 5ms: %d (%.2f%%)\n", stats.Over5ms, float64(stats.Over5ms)/total*100)
}

// sendSetCommand 发送SET命令
func sendSetCommand(conn net.Conn, key, value string) error {
	cmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)

	if _, err := conn.Write([]byte(cmd)); err != nil {
		return err
	}

	buffer := make([]byte, 1024)
	_, err := conn.Read(buffer)
	return err
}

// sendGetCommand 发送GET命令
func sendGetCommand(conn net.Conn, key string) error {
	cmd := fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)

	if _, err := conn.Write([]byte(cmd)); err != nil {
		return err
	}

	buffer := make([]byte, 1024)
	_, err := conn.Read(buffer)
	return err
}
