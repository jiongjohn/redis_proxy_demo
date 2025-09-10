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
	fmt.Println("🔥 Redis Proxy 热Key压力测试")
	fmt.Println("===============================")

	// 测试参数
	const (
		host           = "127.0.0.1:6380"
		numClients     = 100              // 并发客户端数
		testDuration   = 60 * time.Second // 测试持续时间
		hotKeyRatio    = 0.8              // 热Key访问比例 (80%)
		numHotKeys     = 10               // 热Key数量
		numColdKeys    = 1000             // 冷Key数量
		reportInterval = 5 * time.Second  // 报告间隔
	)

	fmt.Printf("测试配置:\n")
	fmt.Printf("├── 目标服务器: %s\n", host)
	fmt.Printf("├── 并发客户端: %d\n", numClients)
	fmt.Printf("├── 测试时长: %v\n", testDuration)
	fmt.Printf("├── 热Key比例: %.1f%%\n", hotKeyRatio*100)
	fmt.Printf("├── 热Key数量: %d\n", numHotKeys)
	fmt.Printf("├── 冷Key数量: %d\n", numColdKeys)
	fmt.Printf("└── 报告间隔: %v\n", reportInterval)
	fmt.Println()

	// 预热数据
	fmt.Println("🔧 预热数据...")
	if err := warmupData(host, numHotKeys, numColdKeys); err != nil {
		fmt.Printf("❌ 预热数据失败: %v\n", err)
		return
	}
	fmt.Println("✅ 数据预热完成")
	fmt.Println()

	// 统计变量
	var stats BenchmarkStats

	// 启动统计报告协程
	stopReporting := make(chan bool)
	go reportStats(&stats, reportInterval, stopReporting)

	// 启动测试
	fmt.Println("🚀 开始压力测试...")
	startTime := time.Now()

	var wg sync.WaitGroup
	stopTesting := make(chan bool)

	// 启动客户端协程
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			runClient(clientID, host, &stats, stopTesting, numHotKeys, numColdKeys, hotKeyRatio)
		}(i)
	}

	// 等待测试时间结束
	time.Sleep(testDuration)
	close(stopTesting)
	wg.Wait()

	// 停止报告
	close(stopReporting)

	// 计算最终统计
	duration := time.Since(startTime)
	printFinalStats(&stats, duration)
}

// BenchmarkStats 基准测试统计
type BenchmarkStats struct {
	// 请求统计
	TotalRequests   int64
	HotKeyRequests  int64
	ColdKeyRequests int64
	GetRequests     int64
	SetRequests     int64

	// 错误统计
	TotalErrors      int64
	ConnectionErrors int64
	TimeoutErrors    int64

	// 延迟统计
	TotalLatency   int64
	HotKeyLatency  int64
	ColdKeyLatency int64
	MinLatency     int64
	MaxLatency     int64

	// 响应时间分布
	LatencyUnder1ms  int64
	LatencyUnder5ms  int64
	LatencyUnder10ms int64
	LatencyUnder50ms int64
	LatencyOver50ms  int64
}

// warmupData 预热数据
func warmupData(host string, numHotKeys, numColdKeys int) error {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		return fmt.Errorf("连接失败: %w", err)
	}
	defer conn.Close()

	// 设置热Key数据
	for i := 0; i < numHotKeys; i++ {
		key := fmt.Sprintf("hotkey_%d", i)
		value := fmt.Sprintf("hotvalue_%d_%d", i, time.Now().Unix())
		if err := sendSetCommand(conn, key, value); err != nil {
			return fmt.Errorf("设置热Key失败: %w", err)
		}
	}

	// 设置冷Key数据
	for i := 0; i < numColdKeys; i++ {
		key := fmt.Sprintf("coldkey_%d", i)
		value := fmt.Sprintf("coldvalue_%d_%d", i, time.Now().Unix())
		if err := sendSetCommand(conn, key, value); err != nil {
			return fmt.Errorf("设置冷Key失败: %w", err)
		}
	}

	return nil
}

// runClient 运行客户端测试
func runClient(clientID int, host string, stats *BenchmarkStats, stopTesting <-chan bool,
	numHotKeys, numColdKeys int, hotKeyRatio float64) {

	// 建立连接
	conn, err := net.Dial("tcp", host)
	if err != nil {
		atomic.AddInt64(&stats.ConnectionErrors, 1)
		atomic.AddInt64(&stats.TotalErrors, 1)
		return
	}
	defer conn.Close()

	// 设置连接超时
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	rand.Seed(time.Now().UnixNano() + int64(clientID))

	for {
		select {
		case <-stopTesting:
			return
		default:
			// 决定访问热Key还是冷Key
			var key string
			var isHotKey bool

			if rand.Float64() < hotKeyRatio {
				// 访问热Key
				hotKeyIndex := rand.Intn(numHotKeys)
				key = fmt.Sprintf("hotkey_%d", hotKeyIndex)
				isHotKey = true
			} else {
				// 访问冷Key
				coldKeyIndex := rand.Intn(numColdKeys)
				key = fmt.Sprintf("coldkey_%d", coldKeyIndex)
				isHotKey = false
			}

			// 随机选择GET或SET操作 (80% GET, 20% SET)
			if rand.Float64() < 0.8 {
				// GET操作
				executeGet(conn, key, isHotKey, stats)
			} else {
				// SET操作
				value := fmt.Sprintf("updated_%s_%d", key, time.Now().UnixNano())
				executeSet(conn, key, value, isHotKey, stats)
			}

			// 重置超时
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		}
	}
}

// executeGet 执行GET操作
func executeGet(conn net.Conn, key string, isHotKey bool, stats *BenchmarkStats) {
	start := time.Now()
	err := sendGetCommand(conn, key)
	latency := time.Since(start).Nanoseconds()

	atomic.AddInt64(&stats.TotalRequests, 1)
	atomic.AddInt64(&stats.GetRequests, 1)

	if err != nil {
		atomic.AddInt64(&stats.TotalErrors, 1)
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			atomic.AddInt64(&stats.TimeoutErrors, 1)
		}
		return
	}

	// 更新延迟统计
	atomic.AddInt64(&stats.TotalLatency, latency)
	if isHotKey {
		atomic.AddInt64(&stats.HotKeyRequests, 1)
		atomic.AddInt64(&stats.HotKeyLatency, latency)
	} else {
		atomic.AddInt64(&stats.ColdKeyRequests, 1)
		atomic.AddInt64(&stats.ColdKeyLatency, latency)
	}

	updateLatencyStats(stats, latency)
}

// executeSet 执行SET操作
func executeSet(conn net.Conn, key, value string, isHotKey bool, stats *BenchmarkStats) {
	start := time.Now()
	err := sendSetCommand(conn, key, value)
	latency := time.Since(start).Nanoseconds()

	atomic.AddInt64(&stats.TotalRequests, 1)
	atomic.AddInt64(&stats.SetRequests, 1)

	if err != nil {
		atomic.AddInt64(&stats.TotalErrors, 1)
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			atomic.AddInt64(&stats.TimeoutErrors, 1)
		}
		return
	}

	// 更新延迟统计
	atomic.AddInt64(&stats.TotalLatency, latency)
	if isHotKey {
		atomic.AddInt64(&stats.HotKeyRequests, 1)
		atomic.AddInt64(&stats.HotKeyLatency, latency)
	} else {
		atomic.AddInt64(&stats.ColdKeyRequests, 1)
		atomic.AddInt64(&stats.ColdKeyLatency, latency)
	}

	updateLatencyStats(stats, latency)
}

// updateLatencyStats 更新延迟统计
func updateLatencyStats(stats *BenchmarkStats, latency int64) {
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

	// 更新延迟分布
	latencyMs := float64(latency) / 1000000
	if latencyMs < 1 {
		atomic.AddInt64(&stats.LatencyUnder1ms, 1)
	} else if latencyMs < 5 {
		atomic.AddInt64(&stats.LatencyUnder5ms, 1)
	} else if latencyMs < 10 {
		atomic.AddInt64(&stats.LatencyUnder10ms, 1)
	} else if latencyMs < 50 {
		atomic.AddInt64(&stats.LatencyUnder50ms, 1)
	} else {
		atomic.AddInt64(&stats.LatencyOver50ms, 1)
	}
}

// reportStats 定期报告统计信息
func reportStats(stats *BenchmarkStats, interval time.Duration, stop <-chan bool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			printCurrentStats(stats)
		}
	}
}

// printCurrentStats 打印当前统计信息
func printCurrentStats(stats *BenchmarkStats) {
	totalReqs := atomic.LoadInt64(&stats.TotalRequests)
	hotKeyReqs := atomic.LoadInt64(&stats.HotKeyRequests)
	coldKeyReqs := atomic.LoadInt64(&stats.ColdKeyRequests)
	totalErrors := atomic.LoadInt64(&stats.TotalErrors)

	if totalReqs == 0 {
		return
	}

	fmt.Printf("\n📊 实时统计 [%s]\n", time.Now().Format("15:04:05"))
	fmt.Printf("├── 总请求: %d (错误: %d, 成功率: %.2f%%)\n",
		totalReqs, totalErrors, float64(totalReqs-totalErrors)/float64(totalReqs)*100)
	fmt.Printf("├── 热Key请求: %d (%.1f%%)\n",
		hotKeyReqs, float64(hotKeyReqs)/float64(totalReqs)*100)
	fmt.Printf("├── 冷Key请求: %d (%.1f%%)\n",
		coldKeyReqs, float64(coldKeyReqs)/float64(totalReqs)*100)

	// 计算平均延迟
	if hotKeyReqs > 0 {
		avgHotLatency := float64(atomic.LoadInt64(&stats.HotKeyLatency)) / float64(hotKeyReqs) / 1000000
		fmt.Printf("├── 热Key平均延迟: %.2f ms\n", avgHotLatency)
	}
	if coldKeyReqs > 0 {
		avgColdLatency := float64(atomic.LoadInt64(&stats.ColdKeyLatency)) / float64(coldKeyReqs) / 1000000
		fmt.Printf("└── 冷Key平均延迟: %.2f ms\n", avgColdLatency)
	}
}

// printFinalStats 打印最终统计信息
func printFinalStats(stats *BenchmarkStats, duration time.Duration) {
	totalReqs := atomic.LoadInt64(&stats.TotalRequests)
	hotKeyReqs := atomic.LoadInt64(&stats.HotKeyRequests)
	coldKeyReqs := atomic.LoadInt64(&stats.ColdKeyRequests)
	getReqs := atomic.LoadInt64(&stats.GetRequests)
	setReqs := atomic.LoadInt64(&stats.SetRequests)
	totalErrors := atomic.LoadInt64(&stats.TotalErrors)
	connErrors := atomic.LoadInt64(&stats.ConnectionErrors)
	timeoutErrors := atomic.LoadInt64(&stats.TimeoutErrors)

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("🏁 最终测试结果")
	fmt.Println(strings.Repeat("=", 60))

	// 基础统计
	fmt.Printf("📈 请求统计:\n")
	fmt.Printf("├── 总请求数: %d\n", totalReqs)
	fmt.Printf("├── GET请求: %d (%.1f%%)\n", getReqs, float64(getReqs)/float64(totalReqs)*100)
	fmt.Printf("├── SET请求: %d (%.1f%%)\n", setReqs, float64(setReqs)/float64(totalReqs)*100)
	fmt.Printf("├── 热Key请求: %d (%.1f%%)\n", hotKeyReqs, float64(hotKeyReqs)/float64(totalReqs)*100)
	fmt.Printf("└── 冷Key请求: %d (%.1f%%)\n", coldKeyReqs, float64(coldKeyReqs)/float64(totalReqs)*100)

	// 错误统计
	fmt.Printf("\n❌ 错误统计:\n")
	fmt.Printf("├── 总错误数: %d (%.2f%%)\n", totalErrors, float64(totalErrors)/float64(totalReqs)*100)
	fmt.Printf("├── 连接错误: %d\n", connErrors)
	fmt.Printf("└── 超时错误: %d\n", timeoutErrors)

	// 性能统计
	fmt.Printf("\n⚡ 性能统计:\n")
	fmt.Printf("├── 测试时长: %v\n", duration)
	fmt.Printf("├── 总吞吐量: %.2f req/s\n", float64(totalReqs)/duration.Seconds())
	fmt.Printf("├── GET吞吐量: %.2f req/s\n", float64(getReqs)/duration.Seconds())
	fmt.Printf("└── SET吞吐量: %.2f req/s\n", float64(setReqs)/duration.Seconds())

	// 延迟统计
	if totalReqs > 0 {
		avgLatency := float64(atomic.LoadInt64(&stats.TotalLatency)) / float64(totalReqs) / 1000000
		minLatency := float64(atomic.LoadInt64(&stats.MinLatency)) / 1000000
		maxLatency := float64(atomic.LoadInt64(&stats.MaxLatency)) / 1000000

		fmt.Printf("\n🕐 延迟统计:\n")
		fmt.Printf("├── 平均延迟: %.2f ms\n", avgLatency)
		fmt.Printf("├── 最小延迟: %.2f ms\n", minLatency)
		fmt.Printf("└── 最大延迟: %.2f ms\n", maxLatency)

		if hotKeyReqs > 0 {
			avgHotLatency := float64(atomic.LoadInt64(&stats.HotKeyLatency)) / float64(hotKeyReqs) / 1000000
			fmt.Printf("├── 热Key平均延迟: %.2f ms\n", avgHotLatency)
		}
		if coldKeyReqs > 0 {
			avgColdLatency := float64(atomic.LoadInt64(&stats.ColdKeyLatency)) / float64(coldKeyReqs) / 1000000
			fmt.Printf("└── 冷Key平均延迟: %.2f ms\n", avgColdLatency)
		}
	}

	// 延迟分布
	fmt.Printf("\n📊 延迟分布:\n")
	fmt.Printf("├── < 1ms:  %d (%.1f%%)\n",
		atomic.LoadInt64(&stats.LatencyUnder1ms),
		float64(atomic.LoadInt64(&stats.LatencyUnder1ms))/float64(totalReqs)*100)
	fmt.Printf("├── < 5ms:  %d (%.1f%%)\n",
		atomic.LoadInt64(&stats.LatencyUnder5ms),
		float64(atomic.LoadInt64(&stats.LatencyUnder5ms))/float64(totalReqs)*100)
	fmt.Printf("├── < 10ms: %d (%.1f%%)\n",
		atomic.LoadInt64(&stats.LatencyUnder10ms),
		float64(atomic.LoadInt64(&stats.LatencyUnder10ms))/float64(totalReqs)*100)
	fmt.Printf("├── < 50ms: %d (%.1f%%)\n",
		atomic.LoadInt64(&stats.LatencyUnder50ms),
		float64(atomic.LoadInt64(&stats.LatencyUnder50ms))/float64(totalReqs)*100)
	fmt.Printf("└── > 50ms: %d (%.1f%%)\n",
		atomic.LoadInt64(&stats.LatencyOver50ms),
		float64(atomic.LoadInt64(&stats.LatencyOver50ms))/float64(totalReqs)*100)

	// 缓存效果分析
	if hotKeyReqs > 0 && coldKeyReqs > 0 {
		hotAvgLatency := float64(atomic.LoadInt64(&stats.HotKeyLatency)) / float64(hotKeyReqs) / 1000000
		coldAvgLatency := float64(atomic.LoadInt64(&stats.ColdKeyLatency)) / float64(coldKeyReqs) / 1000000
		improvement := (coldAvgLatency - hotAvgLatency) / coldAvgLatency * 100

		fmt.Printf("\n🎯 缓存效果分析:\n")
		fmt.Printf("├── 热Key延迟: %.2f ms\n", hotAvgLatency)
		fmt.Printf("├── 冷Key延迟: %.2f ms\n", coldAvgLatency)
		fmt.Printf("└── 性能提升: %.1f%%\n", improvement)
	}

	fmt.Println(strings.Repeat("=", 60))
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
