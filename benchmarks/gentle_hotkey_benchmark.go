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
	fmt.Println("🎯 Redis Proxy 温和热Key压力测试")
	fmt.Println("=================================")

	// 测试参数
	const (
		host           = "127.0.0.1:6380"
		numClients     = 20               // 较少的并发客户端
		testDuration   = 30 * time.Second // 测试持续时间
		numHotKeys     = 5                // 5个热Key
		reportInterval = 3 * time.Second  // 报告间隔
		readRatio      = 0.95             // 读操作比例 (95% GET, 5% SET)
		requestDelay   = 50               // 请求间隔微秒
	)

	fmt.Printf("测试配置:\n")
	fmt.Printf("├── 目标服务器: %s\n", host)
	fmt.Printf("├── 并发客户端: %d (温和压力)\n", numClients)
	fmt.Printf("├── 测试时长: %v\n", testDuration)
	fmt.Printf("├── 热Key数量: %d (会被缓存)\n", numHotKeys)
	fmt.Printf("├── 冷Key数量: %d (nocache:前缀，不会被缓存)\n", numHotKeys)
	fmt.Printf("├── 热Key访问比例: 80%% (缓存测试)\n")
	fmt.Printf("├── 冷Key访问比例: 20%% (no_cache_prefix测试)\n")
	fmt.Printf("├── 读操作比例: %.1f%%\n", readRatio*100)
	fmt.Printf("├── 请求间隔: %dμs\n", requestDelay)
	fmt.Printf("└── 报告间隔: %v\n", reportInterval)
	fmt.Println()

	// 预热数据
	fmt.Println("🔧 预热热Key数据...")
	if err := warmupGentleHotKeys(host, numHotKeys); err != nil {
		fmt.Printf("❌ 预热数据失败: %v\n", err)
		return
	}
	fmt.Println("✅ 热Key数据预热完成")

	// 等待缓存稳定
	fmt.Println("⏳ 等待缓存稳定...")
	time.Sleep(2 * time.Second)
	fmt.Println()

	// 统计变量
	var stats GentleHotKeyStats

	// 启动统计报告协程
	stopReporting := make(chan bool)
	go reportGentleHotKeyStats(&stats, reportInterval, stopReporting)

	// 启动测试
	fmt.Println("🚀 开始温和热Key压力测试...")
	startTime := time.Now()

	var wg sync.WaitGroup
	stopTesting := make(chan bool)

	// 启动客户端协程
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			runGentleHotKeyClient(clientID, host, &stats, stopTesting, numHotKeys, readRatio, requestDelay)
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
	printGentleHotKeyFinalStats(&stats, duration, numHotKeys)
}

// GentleHotKeyStats 温和热Key基准测试统计
type GentleHotKeyStats struct {
	// 请求统计
	TotalRequests int64
	GetRequests   int64
	SetRequests   int64

	// 错误统计
	TotalErrors      int64
	ConnectionErrors int64
	TimeoutErrors    int64

	// 延迟统计 (纳秒)
	TotalLatency int64
	MinLatency   int64
	MaxLatency   int64

	// 响应时间分布
	Under100us int64 // < 100微秒
	Under200us int64 // < 200微秒
	Under500us int64 // < 500微秒
	Under1ms   int64 // < 1毫秒
	Under2ms   int64 // < 2毫秒
	Under5ms   int64 // < 5毫秒
	Over5ms    int64 // > 5毫秒

	// 每个热Key的统计
	HotKeyStats [10]HotKeyIndividualStats // 支持最多10个热Key
}

// HotKeyIndividualStats 单个热Key统计
type HotKeyIndividualStats struct {
	Requests     int64
	TotalLatency int64
	MinLatency   int64
	MaxLatency   int64
}

// warmupGentleHotKeys 预热温和热Key数据 (冷Key使用nocache:前缀跳过缓存)
func warmupGentleHotKeys(host string, numHotKeys int) error {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		return fmt.Errorf("连接失败: %w", err)
	}
	defer conn.Close()

	// 设置热Key数据 (会被缓存)
	for i := 0; i < numHotKeys; i++ {
		key := fmt.Sprintf("gentle_hotkey_%d", i)
		value := fmt.Sprintf("gentle_hotvalue_%d_stable_data", i)
		if err := sendSetCommand(conn, key, value); err != nil {
			return fmt.Errorf("设置热Key失败: %w", err)
		}
		fmt.Printf("   预热热Key: %s (会被缓存)\n", key)
	}

	// 设置冷Key数据 (使用nocache:前缀，不会被缓存)
	for i := 0; i < numHotKeys; i++ {
		key := fmt.Sprintf("nocache:gentle_coldkey_%d", i)
		value := fmt.Sprintf("gentle_coldvalue_%d_nocache", i)
		if err := sendSetCommand(conn, key, value); err != nil {
			return fmt.Errorf("设置冷Key失败: %w", err)
		}
		fmt.Printf("   预热冷Key: %s (nocache:前缀，不会被缓存)\n", key)
	}

	// 执行多次GET来确保热Key缓存预热
	fmt.Printf("   执行热Key缓存预热GET操作...\n")
	for round := 0; round < 10; round++ {
		for i := 0; i < numHotKeys; i++ {
			key := fmt.Sprintf("gentle_hotkey_%d", i)
			if err := sendGetCommand(conn, key); err != nil {
				return fmt.Errorf("预热GET失败: %w", err)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	return nil
}

// runGentleHotKeyClient 运行温和热Key客户端测试
func runGentleHotKeyClient(clientID int, host string, stats *GentleHotKeyStats, stopTesting <-chan bool,
	numHotKeys int, readRatio float64, requestDelay int) {

	// 建立连接
	conn, err := net.Dial("tcp", host)
	if err != nil {
		atomic.AddInt64(&stats.ConnectionErrors, 1)
		atomic.AddInt64(&stats.TotalErrors, 1)
		return
	}
	defer conn.Close()

	// 设置连接超时
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	conn.SetWriteDeadline(time.Now().Add(30 * time.Second))

	rand.Seed(time.Now().UnixNano() + int64(clientID))

	for {
		select {
		case <-stopTesting:
			return
		default:
			// 80%概率访问热Key，20%概率访问冷Key (nocache:前缀)
			var key string
			var keyIndex int

			if rand.Float64() < 0.8 {
				// 访问热Key (会被缓存)
				keyIndex = rand.Intn(numHotKeys)
				key = fmt.Sprintf("gentle_hotkey_%d", keyIndex)
			} else {
				// 访问冷Key (nocache:前缀，不会被缓存)
				keyIndex = rand.Intn(numHotKeys)
				key = fmt.Sprintf("nocache:gentle_coldkey_%d", keyIndex)
			}

			// 根据读写比例选择操作
			if rand.Float64() < readRatio {
				// GET操作（主要操作）
				executeGentleHotKeyGet(conn, key, keyIndex, stats)
			} else {
				// SET操作（偶尔更新）
				value := fmt.Sprintf("updated_gentle_value_%d_%d", keyIndex, time.Now().UnixNano())
				executeGentleHotKeySet(conn, key, value, keyIndex, stats)
			}

			// 重置超时
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			conn.SetWriteDeadline(time.Now().Add(30 * time.Second))

			// 温和的请求间隔
			time.Sleep(time.Duration(requestDelay) * time.Microsecond)
		}
	}
}

// executeGentleHotKeyGet 执行温和热Key GET操作
func executeGentleHotKeyGet(conn net.Conn, key string, keyIndex int, stats *GentleHotKeyStats) {
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

	// 更新总体延迟统计
	atomic.AddInt64(&stats.TotalLatency, latency)
	updateGentleHotKeyLatencyStats(stats, latency)

	// 更新单个热Key统计
	if keyIndex < len(stats.HotKeyStats) {
		atomic.AddInt64(&stats.HotKeyStats[keyIndex].Requests, 1)
		atomic.AddInt64(&stats.HotKeyStats[keyIndex].TotalLatency, latency)
		updateIndividualKeyLatency(&stats.HotKeyStats[keyIndex], latency)
	}
}

// executeGentleHotKeySet 执行温和热Key SET操作
func executeGentleHotKeySet(conn net.Conn, key, value string, keyIndex int, stats *GentleHotKeyStats) {
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

	// 更新总体延迟统计
	atomic.AddInt64(&stats.TotalLatency, latency)
	updateGentleHotKeyLatencyStats(stats, latency)

	// 更新单个热Key统计
	if keyIndex < len(stats.HotKeyStats) {
		atomic.AddInt64(&stats.HotKeyStats[keyIndex].Requests, 1)
		atomic.AddInt64(&stats.HotKeyStats[keyIndex].TotalLatency, latency)
		updateIndividualKeyLatency(&stats.HotKeyStats[keyIndex], latency)
	}
}

// updateGentleHotKeyLatencyStats 更新温和热Key延迟统计
func updateGentleHotKeyLatencyStats(stats *GentleHotKeyStats, latency int64) {
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

// updateIndividualKeyLatency 更新单个Key延迟统计
func updateIndividualKeyLatency(keyStats *HotKeyIndividualStats, latency int64) {
	// 更新最小延迟
	for {
		current := atomic.LoadInt64(&keyStats.MinLatency)
		if current != 0 && latency >= current {
			break
		}
		if atomic.CompareAndSwapInt64(&keyStats.MinLatency, current, latency) {
			break
		}
	}

	// 更新最大延迟
	for {
		current := atomic.LoadInt64(&keyStats.MaxLatency)
		if latency <= current {
			break
		}
		if atomic.CompareAndSwapInt64(&keyStats.MaxLatency, current, latency) {
			break
		}
	}
}

// reportGentleHotKeyStats 定期报告温和热Key统计信息
func reportGentleHotKeyStats(stats *GentleHotKeyStats, interval time.Duration, stop <-chan bool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			printGentleHotKeyCurrentStats(stats)
		}
	}
}

// printGentleHotKeyCurrentStats 打印当前温和热Key统计信息
func printGentleHotKeyCurrentStats(stats *GentleHotKeyStats) {
	totalReqs := atomic.LoadInt64(&stats.TotalRequests)
	getReqs := atomic.LoadInt64(&stats.GetRequests)
	setReqs := atomic.LoadInt64(&stats.SetRequests)
	totalErrors := atomic.LoadInt64(&stats.TotalErrors)

	if totalReqs == 0 {
		return
	}

	fmt.Printf("\n📊 实时统计 [%s]\n", time.Now().Format("15:04:05"))
	fmt.Printf("├── 总请求: %d (GET: %d, SET: %d, 错误: %d)\n",
		totalReqs, getReqs, setReqs, totalErrors)

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
	under2ms := atomic.LoadInt64(&stats.Under2ms)

	fmt.Printf("├── 延迟分布: <100μs: %.1f%%, <200μs: %.1f%%, <500μs: %.1f%%, <1ms: %.1f%%, <2ms: %.1f%%\n",
		float64(under100us)/float64(totalReqs)*100,
		float64(under200us)/float64(totalReqs)*100,
		float64(under500us)/float64(totalReqs)*100,
		float64(under1ms)/float64(totalReqs)*100,
		float64(under2ms)/float64(totalReqs)*100)

	// 显示超快响应率
	ultraFast := under100us + under200us + under500us
	ultraFastRatio := float64(ultraFast) / float64(totalReqs) * 100
	fmt.Printf("└── 🚀 超快响应率(<500μs): %.1f%%\n", ultraFastRatio)
}

// printGentleHotKeyFinalStats 打印温和热Key最终统计信息
func printGentleHotKeyFinalStats(stats *GentleHotKeyStats, duration time.Duration, numHotKeys int) {
	totalReqs := atomic.LoadInt64(&stats.TotalRequests)
	getReqs := atomic.LoadInt64(&stats.GetRequests)
	setReqs := atomic.LoadInt64(&stats.SetRequests)
	totalErrors := atomic.LoadInt64(&stats.TotalErrors)
	connErrors := atomic.LoadInt64(&stats.ConnectionErrors)
	timeoutErrors := atomic.LoadInt64(&stats.TimeoutErrors)

	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Println("🏁 温和热Key测试最终结果")
	fmt.Println(strings.Repeat("=", 70))

	// 基础统计
	fmt.Printf("📈 请求统计:\n")
	fmt.Printf("├── 总请求数: %d\n", totalReqs)
	fmt.Printf("├── GET请求: %d (%.1f%%)\n", getReqs, float64(getReqs)/float64(totalReqs)*100)
	fmt.Printf("├── SET请求: %d (%.1f%%)\n", setReqs, float64(setReqs)/float64(totalReqs)*100)
	fmt.Printf("└── 成功率: %.3f%%\n", float64(totalReqs-totalErrors)/float64(totalReqs)*100)

	// 错误统计
	if totalErrors > 0 {
		fmt.Printf("\n❌ 错误统计:\n")
		fmt.Printf("├── 总错误数: %d (%.3f%%)\n", totalErrors, float64(totalErrors)/float64(totalReqs)*100)
		fmt.Printf("├── 连接错误: %d\n", connErrors)
		fmt.Printf("└── 超时错误: %d\n", timeoutErrors)
	}

	// 性能统计
	fmt.Printf("\n⚡ 性能统计:\n")
	fmt.Printf("├── 测试时长: %v\n", duration)
	fmt.Printf("├── 总吞吐量: %.2f req/s\n", float64(totalReqs)/duration.Seconds())
	fmt.Printf("├── GET吞吐量: %.2f req/s\n", float64(getReqs)/duration.Seconds())
	fmt.Printf("└── SET吞吐量: %.2f req/s\n", float64(setReqs)/duration.Seconds())

	// 延迟分析
	if totalReqs > 0 {
		avgLatency := float64(atomic.LoadInt64(&stats.TotalLatency)) / float64(totalReqs) / 1000000
		minLatency := float64(atomic.LoadInt64(&stats.MinLatency)) / 1000000
		maxLatency := float64(atomic.LoadInt64(&stats.MaxLatency)) / 1000000

		fmt.Printf("\n🎯 延迟分析:\n")
		fmt.Printf("├── 平均延迟: %.3f ms\n", avgLatency)
		fmt.Printf("├── 最小延迟: %.3f ms\n", minLatency)
		fmt.Printf("└── 最大延迟: %.3f ms\n", maxLatency)

		// 延迟分布
		under100us := atomic.LoadInt64(&stats.Under100us)
		under200us := atomic.LoadInt64(&stats.Under200us)
		under500us := atomic.LoadInt64(&stats.Under500us)
		under1ms := atomic.LoadInt64(&stats.Under1ms)
		under2ms := atomic.LoadInt64(&stats.Under2ms)
		under5ms := atomic.LoadInt64(&stats.Under5ms)
		over5ms := atomic.LoadInt64(&stats.Over5ms)

		fmt.Printf("\n📊 延迟分布:\n")
		fmt.Printf("├── < 100μs:  %d (%.2f%%)\n", under100us, float64(under100us)/float64(totalReqs)*100)
		fmt.Printf("├── < 200μs:  %d (%.2f%%)\n", under200us, float64(under200us)/float64(totalReqs)*100)
		fmt.Printf("├── < 500μs:  %d (%.2f%%)\n", under500us, float64(under500us)/float64(totalReqs)*100)
		fmt.Printf("├── < 1ms:    %d (%.2f%%)\n", under1ms, float64(under1ms)/float64(totalReqs)*100)
		fmt.Printf("├── < 2ms:    %d (%.2f%%)\n", under2ms, float64(under2ms)/float64(totalReqs)*100)
		fmt.Printf("├── < 5ms:    %d (%.2f%%)\n", under5ms, float64(under5ms)/float64(totalReqs)*100)
		fmt.Printf("└── > 5ms:    %d (%.2f%%)\n", over5ms, float64(over5ms)/float64(totalReqs)*100)

		// 缓存效果评估
		ultraFast := under100us + under200us + under500us
		ultraFastRatio := float64(ultraFast) / float64(totalReqs) * 100
		fast := ultraFast + under1ms + under2ms
		fastRatio := float64(fast) / float64(totalReqs) * 100

		fmt.Printf("\n🚀 缓存效果评估:\n")
		fmt.Printf("├── 超快响应(<500μs): %.2f%%\n", ultraFastRatio)
		fmt.Printf("├── 快速响应(<2ms): %.2f%%\n", fastRatio)

		if ultraFastRatio > 80 {
			fmt.Printf("└── 🎉 缓存效果: 优秀 (超快响应率 > 80%%)\n")
		} else if ultraFastRatio > 60 {
			fmt.Printf("└── ✅ 缓存效果: 良好 (超快响应率 > 60%%)\n")
		} else if ultraFastRatio > 40 {
			fmt.Printf("└── ⚠️ 缓存效果: 一般 (超快响应率 > 40%%)\n")
		} else if fastRatio > 80 {
			fmt.Printf("└── ✅ 缓存效果: 良好 (快速响应率 > 80%%)\n")
		} else {
			fmt.Printf("└── ❌ 缓存效果: 需要优化 (快速响应率 < 80%%)\n")
		}
	}

	// 单个热Key统计
	fmt.Printf("\n🔥 热Key详细统计:\n")
	for i := 0; i < numHotKeys; i++ {
		keyStats := &stats.HotKeyStats[i]
		requests := atomic.LoadInt64(&keyStats.Requests)
		if requests > 0 {
			avgLatency := float64(atomic.LoadInt64(&keyStats.TotalLatency)) / float64(requests) / 1000000
			minLatency := float64(atomic.LoadInt64(&keyStats.MinLatency)) / 1000000
			maxLatency := float64(atomic.LoadInt64(&keyStats.MaxLatency)) / 1000000

			fmt.Printf("├── gentle_hotkey_%d: %d 请求, 平均 %.3f ms (%.3f - %.3f ms)\n",
				i, requests, avgLatency, minLatency, maxLatency)
		}
	}

	fmt.Println(strings.Repeat("=", 70))
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
