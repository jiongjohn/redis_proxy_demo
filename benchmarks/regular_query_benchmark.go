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
	fmt.Println("📊 Redis Proxy 常规查询压力测试")
	fmt.Println("=================================")

	// 测试参数
	const (
		host           = "127.0.0.1:6380"
		numClients     = 50               // 并发客户端数
		testDuration   = 30 * time.Second // 测试持续时间
		keySpaceSize   = 10000            // Key空间大小
		reportInterval = 5 * time.Second  // 报告间隔
	)

	fmt.Printf("测试配置:\n")
	fmt.Printf("├── 目标服务器: %s\n", host)
	fmt.Printf("├── 并发客户端: %d\n", numClients)
	fmt.Printf("├── 测试时长: %v\n", testDuration)
	fmt.Printf("├── Key空间大小: %d\n", keySpaceSize)
	fmt.Printf("└── 报告间隔: %v\n", reportInterval)
	fmt.Println()

	// 运行多种测试场景
	scenarios := []TestScenario{
		{
			Name:        "纯GET测试",
			Description: "100% GET操作，测试读取性能",
			GetRatio:    1.0,
			SetRatio:    0.0,
		},
		{
			Name:        "纯SET测试",
			Description: "100% SET操作，测试写入性能",
			GetRatio:    0.0,
			SetRatio:    1.0,
		},
		{
			Name:        "读写混合测试",
			Description: "80% GET + 20% SET，模拟真实场景",
			GetRatio:    0.8,
			SetRatio:    0.2,
		},
		{
			Name:        "重读测试",
			Description: "95% GET + 5% SET，读多写少场景",
			GetRatio:    0.95,
			SetRatio:    0.05,
		},
		{
			Name:        "重写测试",
			Description: "30% GET + 70% SET，写多读少场景",
			GetRatio:    0.3,
			SetRatio:    0.7,
		},
	}

	// 预热数据
	fmt.Println("🔧 预热数据...")
	if err := warmupRegularData(host, keySpaceSize); err != nil {
		fmt.Printf("❌ 预热数据失败: %v\n", err)
		return
	}
	fmt.Println("✅ 数据预热完成")
	fmt.Println()

	// 运行各种测试场景
	for i, scenario := range scenarios {
		fmt.Printf("🧪 测试场景 %d/%d: %s\n", i+1, len(scenarios), scenario.Name)
		fmt.Printf("   %s\n", scenario.Description)
		fmt.Println()

		runScenario(host, numClients, testDuration, keySpaceSize, reportInterval, scenario)

		if i < len(scenarios)-1 {
			fmt.Println("\n" + strings.Repeat("=", 60))
			fmt.Println("⏳ 等待5秒后开始下一个测试...")
			time.Sleep(5 * time.Second)
			fmt.Println()
		}
	}

	fmt.Println("\n🎉 所有测试场景完成！")
}

// TestScenario 测试场景
type TestScenario struct {
	Name        string
	Description string
	GetRatio    float64
	SetRatio    float64
}

// RegularStats 常规测试统计
type RegularStats struct {
	// 请求统计
	TotalRequests int64
	GetRequests   int64
	SetRequests   int64

	// 错误统计
	TotalErrors      int64
	ConnectionErrors int64
	TimeoutErrors    int64

	// 延迟统计
	TotalLatency int64
	GetLatency   int64
	SetLatency   int64
	MinLatency   int64
	MaxLatency   int64

	// 响应时间分布
	LatencyUnder1ms  int64
	LatencyUnder5ms  int64
	LatencyUnder10ms int64
	LatencyUnder50ms int64
	LatencyOver50ms  int64

	// 吞吐量统计
	LastReportTime    time.Time
	LastReportReqs    int64
	CurrentThroughput float64
}

// warmupRegularData 预热常规数据
func warmupRegularData(host string, keySpaceSize int) error {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		return fmt.Errorf("连接失败: %w", err)
	}
	defer conn.Close()

	// 预热一部分数据 (20%)
	warmupSize := keySpaceSize / 5
	for i := 0; i < warmupSize; i++ {
		key := fmt.Sprintf("regular_key_%d", i)
		value := fmt.Sprintf("regular_value_%d_%d", i, time.Now().Unix())
		if err := sendSetCommand(conn, key, value); err != nil {
			return fmt.Errorf("设置Key失败: %w", err)
		}

		// 每100个key打印一次进度
		if (i+1)%100 == 0 {
			fmt.Printf("   预热进度: %d/%d (%.1f%%)\r", i+1, warmupSize, float64(i+1)/float64(warmupSize)*100)
		}
	}
	fmt.Printf("   预热进度: %d/%d (100.0%%)\n", warmupSize, warmupSize)

	return nil
}

// runScenario 运行测试场景
func runScenario(host string, numClients int, testDuration time.Duration,
	keySpaceSize int, reportInterval time.Duration, scenario TestScenario) {

	var stats RegularStats
	stats.LastReportTime = time.Now()

	// 启动统计报告协程
	stopReporting := make(chan bool)
	go reportRegularStats(&stats, reportInterval, stopReporting)

	// 启动测试
	startTime := time.Now()
	var wg sync.WaitGroup
	stopTesting := make(chan bool)

	// 启动客户端协程
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			runRegularClient(clientID, host, &stats, stopTesting, keySpaceSize, scenario)
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
	printRegularFinalStats(&stats, duration, scenario)
}

// runRegularClient 运行常规客户端测试
func runRegularClient(clientID int, host string, stats *RegularStats,
	stopTesting <-chan bool, keySpaceSize int, scenario TestScenario) {

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
			// 随机选择Key
			keyIndex := rand.Intn(keySpaceSize)
			key := fmt.Sprintf("regular_key_%d", keyIndex)

			// 根据场景比例选择操作
			if rand.Float64() < scenario.GetRatio {
				// GET操作
				executeRegularGet(conn, key, stats)
			} else {
				// SET操作
				value := fmt.Sprintf("updated_value_%d_%d", keyIndex, time.Now().UnixNano())
				executeRegularSet(conn, key, value, stats)
			}

			// 重置超时
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		}
	}
}

// executeRegularGet 执行常规GET操作
func executeRegularGet(conn net.Conn, key string, stats *RegularStats) {
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
	atomic.AddInt64(&stats.GetLatency, latency)
	updateRegularLatencyStats(stats, latency)
}

// executeRegularSet 执行常规SET操作
func executeRegularSet(conn net.Conn, key, value string, stats *RegularStats) {
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
	atomic.AddInt64(&stats.SetLatency, latency)
	updateRegularLatencyStats(stats, latency)
}

// updateRegularLatencyStats 更新常规延迟统计
func updateRegularLatencyStats(stats *RegularStats, latency int64) {
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

// reportRegularStats 定期报告常规统计信息
func reportRegularStats(stats *RegularStats, interval time.Duration, stop <-chan bool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			printRegularCurrentStats(stats)
		}
	}
}

// printRegularCurrentStats 打印当前常规统计信息
func printRegularCurrentStats(stats *RegularStats) {
	now := time.Now()
	totalReqs := atomic.LoadInt64(&stats.TotalRequests)
	getReqs := atomic.LoadInt64(&stats.GetRequests)
	setReqs := atomic.LoadInt64(&stats.SetRequests)
	totalErrors := atomic.LoadInt64(&stats.TotalErrors)

	if totalReqs == 0 {
		return
	}

	// 计算当前吞吐量
	timeDiff := now.Sub(stats.LastReportTime).Seconds()
	reqDiff := totalReqs - stats.LastReportReqs
	currentThroughput := float64(reqDiff) / timeDiff

	stats.LastReportTime = now
	stats.LastReportReqs = totalReqs
	stats.CurrentThroughput = currentThroughput

	fmt.Printf("\n📊 实时统计 [%s]\n", now.Format("15:04:05"))
	fmt.Printf("├── 总请求: %d (GET: %d, SET: %d)\n", totalReqs, getReqs, setReqs)
	fmt.Printf("├── 错误数: %d (成功率: %.2f%%)\n",
		totalErrors, float64(totalReqs-totalErrors)/float64(totalReqs)*100)
	fmt.Printf("└── 当前吞吐量: %.2f req/s\n", currentThroughput)

	// 计算平均延迟
	if getReqs > 0 {
		avgGetLatency := float64(atomic.LoadInt64(&stats.GetLatency)) / float64(getReqs) / 1000000
		fmt.Printf("├── GET平均延迟: %.2f ms\n", avgGetLatency)
	}
	if setReqs > 0 {
		avgSetLatency := float64(atomic.LoadInt64(&stats.SetLatency)) / float64(setReqs) / 1000000
		fmt.Printf("└── SET平均延迟: %.2f ms\n", avgSetLatency)
	}
}

// printRegularFinalStats 打印常规最终统计信息
func printRegularFinalStats(stats *RegularStats, duration time.Duration, scenario TestScenario) {
	totalReqs := atomic.LoadInt64(&stats.TotalRequests)
	getReqs := atomic.LoadInt64(&stats.GetRequests)
	setReqs := atomic.LoadInt64(&stats.SetRequests)
	totalErrors := atomic.LoadInt64(&stats.TotalErrors)
	connErrors := atomic.LoadInt64(&stats.ConnectionErrors)
	timeoutErrors := atomic.LoadInt64(&stats.TimeoutErrors)

	fmt.Println("\n" + strings.Repeat("=", 50))
	fmt.Printf("🏁 %s - 最终结果\n", scenario.Name)
	fmt.Println(strings.Repeat("=", 50))

	// 基础统计
	fmt.Printf("📈 请求统计:\n")
	fmt.Printf("├── 总请求数: %d\n", totalReqs)
	fmt.Printf("├── GET请求: %d (%.1f%%)\n", getReqs, float64(getReqs)/float64(totalReqs)*100)
	fmt.Printf("└── SET请求: %d (%.1f%%)\n", setReqs, float64(setReqs)/float64(totalReqs)*100)

	// 错误统计
	fmt.Printf("\n❌ 错误统计:\n")
	fmt.Printf("├── 总错误数: %d (%.2f%%)\n", totalErrors, float64(totalErrors)/float64(totalReqs)*100)
	fmt.Printf("├── 连接错误: %d\n", connErrors)
	fmt.Printf("└── 超时错误: %d\n", timeoutErrors)

	// 性能统计
	fmt.Printf("\n⚡ 性能统计:\n")
	fmt.Printf("├── 测试时长: %v\n", duration)
	fmt.Printf("├── 总吞吐量: %.2f req/s\n", float64(totalReqs)/duration.Seconds())
	if getReqs > 0 {
		fmt.Printf("├── GET吞吐量: %.2f req/s\n", float64(getReqs)/duration.Seconds())
	}
	if setReqs > 0 {
		fmt.Printf("└── SET吞吐量: %.2f req/s\n", float64(setReqs)/duration.Seconds())
	}

	// 延迟统计
	if totalReqs > 0 {
		avgLatency := float64(atomic.LoadInt64(&stats.TotalLatency)) / float64(totalReqs) / 1000000
		minLatency := float64(atomic.LoadInt64(&stats.MinLatency)) / 1000000
		maxLatency := float64(atomic.LoadInt64(&stats.MaxLatency)) / 1000000

		fmt.Printf("\n🕐 延迟统计:\n")
		fmt.Printf("├── 平均延迟: %.2f ms\n", avgLatency)
		fmt.Printf("├── 最小延迟: %.2f ms\n", minLatency)
		fmt.Printf("└── 最大延迟: %.2f ms\n", maxLatency)

		if getReqs > 0 {
			avgGetLatency := float64(atomic.LoadInt64(&stats.GetLatency)) / float64(getReqs) / 1000000
			fmt.Printf("├── GET平均延迟: %.2f ms\n", avgGetLatency)
		}
		if setReqs > 0 {
			avgSetLatency := float64(atomic.LoadInt64(&stats.SetLatency)) / float64(setReqs) / 1000000
			fmt.Printf("└── SET平均延迟: %.2f ms\n", avgSetLatency)
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

	fmt.Println(strings.Repeat("=", 50))
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
