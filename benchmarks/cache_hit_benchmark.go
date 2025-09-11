package main

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	fmt.Println("🎯 Redis Proxy 缓存命中专项测试")
	fmt.Println("===============================")

	const (
		host      = "127.0.0.1:6380"
		testKey   = "cache_test_key"
		testValue = "cache_test_value_stable"
	)

	fmt.Printf("测试配置:\n")
	fmt.Printf("├── 目标服务器: %s\n", host)
	fmt.Printf("├── 测试Key: %s\n", testKey)
	fmt.Printf("└── 测试Value: %s\n", testValue)
	fmt.Println()

	// 第一阶段：预热缓存
	fmt.Println("🔧 第一阶段：预热缓存")
	if err := warmupCache(host, testKey, testValue); err != nil {
		fmt.Printf("❌ 预热失败: %v\n", err)
		return
	}
	fmt.Println("✅ 缓存预热完成")
	fmt.Println()

	// 第二阶段：单连接延迟测试
	fmt.Println("📏 第二阶段：单连接延迟测试")
	singleConnLatency, err := testSingleConnectionLatency(host, testKey, 100)
	if err != nil {
		fmt.Printf("❌ 单连接测试失败: %v\n", err)
		return
	}
	fmt.Printf("✅ 单连接平均延迟: %.3f ms\n", singleConnLatency)
	fmt.Println()

	// 第三阶段：并发缓存命中测试
	fmt.Println("🚀 第三阶段：并发缓存命中测试")
	concurrentLatency, throughput, err := testConcurrentCacheHits(host, testKey, 20, 5*time.Second)
	if err != nil {
		fmt.Printf("❌ 并发测试失败: %v\n", err)
		return
	}
	fmt.Printf("✅ 并发平均延迟: %.3f ms\n", concurrentLatency)
	fmt.Printf("✅ 并发吞吐量: %.2f req/s\n", throughput)
	fmt.Println()

	// 第四阶段：缓存失效后的延迟对比
	fmt.Println("🔄 第四阶段：缓存失效后延迟对比")
	if err := testCacheInvalidation(host, testKey, testValue); err != nil {
		fmt.Printf("❌ 缓存失效测试失败: %v\n", err)
		return
	}
	fmt.Println()

	// 第五阶段：缓存 vs no_cache_prefix 对比
	fmt.Println("🆚 第五阶段：缓存 vs no_cache_prefix 延迟对比")
	if err := testHotVsColdKey(host, testKey); err != nil {
		fmt.Printf("❌ 缓存vs非缓存对比测试失败: %v\n", err)
		return
	}

	fmt.Println("\n🎉 所有测试完成！")
}

// warmupCache 预热缓存
func warmupCache(host, key, value string) error {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		return fmt.Errorf("连接失败: %w", err)
	}
	defer conn.Close()

	// 设置Key
	if err := sendSetCommand(conn, key, value); err != nil {
		return fmt.Errorf("SET失败: %w", err)
	}

	// 执行几次GET来确保缓存
	for i := 0; i < 5; i++ {
		if err := sendGetCommand(conn, key); err != nil {
			return fmt.Errorf("GET失败: %w", err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

// testSingleConnectionLatency 测试单连接延迟
func testSingleConnectionLatency(host, key string, iterations int) (float64, error) {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		return 0, fmt.Errorf("连接失败: %w", err)
	}
	defer conn.Close()

	var totalLatency int64
	var successCount int

	fmt.Printf("   执行 %d 次GET操作...\n", iterations)

	for i := 0; i < iterations; i++ {
		start := time.Now()
		err := sendGetCommand(conn, key)
		latency := time.Since(start)

		if err != nil {
			fmt.Printf("   第%d次GET失败: %v\n", i+1, err)
			continue
		}

		totalLatency += latency.Nanoseconds()
		successCount++

		// 每10次显示进度
		if (i+1)%10 == 0 {
			avgLatency := float64(totalLatency) / float64(successCount) / 1000000
			fmt.Printf("   进度: %d/%d, 当前平均延迟: %.3f ms\n", i+1, iterations, avgLatency)
		}
	}

	if successCount == 0 {
		return 0, fmt.Errorf("所有请求都失败了")
	}

	avgLatency := float64(totalLatency) / float64(successCount) / 1000000
	return avgLatency, nil
}

// testConcurrentCacheHits 测试并发缓存命中
func testConcurrentCacheHits(host, key string, numClients int, duration time.Duration) (float64, float64, error) {
	var (
		totalRequests int64
		totalLatency  int64
		totalErrors   int64
	)

	var wg sync.WaitGroup
	stopTesting := make(chan bool)

	fmt.Printf("   启动 %d 个并发客户端，持续 %v...\n", numClients, duration)

	startTime := time.Now()

	// 启动客户端
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", host)
			if err != nil {
				atomic.AddInt64(&totalErrors, 1)
				return
			}
			defer conn.Close()

			for {
				select {
				case <-stopTesting:
					return
				default:
					start := time.Now()
					err := sendGetCommand(conn, key)
					latency := time.Since(start)

					atomic.AddInt64(&totalRequests, 1)

					if err != nil {
						atomic.AddInt64(&totalErrors, 1)
					} else {
						atomic.AddInt64(&totalLatency, latency.Nanoseconds())
					}

					// 小延迟避免过度压力
					time.Sleep(time.Microsecond * 50)
				}
			}
		}(i)
	}

	// 等待测试时间
	time.Sleep(duration)
	close(stopTesting)
	wg.Wait()

	actualDuration := time.Since(startTime)
	requests := atomic.LoadInt64(&totalRequests)
	errors := atomic.LoadInt64(&totalErrors)
	latency := atomic.LoadInt64(&totalLatency)

	fmt.Printf("   总请求: %d, 错误: %d, 成功率: %.2f%%\n",
		requests, errors, float64(requests-errors)/float64(requests)*100)

	if requests-errors == 0 {
		return 0, 0, fmt.Errorf("没有成功的请求")
	}

	avgLatency := float64(latency) / float64(requests-errors) / 1000000
	throughput := float64(requests) / actualDuration.Seconds()

	return avgLatency, throughput, nil
}

// testCacheInvalidation 测试缓存失效
func testCacheInvalidation(host, key, value string) error {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		return fmt.Errorf("连接失败: %w", err)
	}
	defer conn.Close()

	// 测试缓存命中延迟
	fmt.Printf("   测试缓存命中延迟...\n")
	var cachedLatencies []time.Duration
	for i := 0; i < 10; i++ {
		start := time.Now()
		err := sendGetCommand(conn, key)
		latency := time.Since(start)
		if err != nil {
			return fmt.Errorf("GET失败: %w", err)
		}
		cachedLatencies = append(cachedLatencies, latency)
		time.Sleep(10 * time.Millisecond)
	}

	// 删除Key（使缓存失效）
	fmt.Printf("   删除Key使缓存失效...\n")
	if err := sendDelCommand(conn, key); err != nil {
		return fmt.Errorf("DEL失败: %w", err)
	}

	// 等待一下确保删除生效
	time.Sleep(100 * time.Millisecond)

	// 重新设置Key
	if err := sendSetCommand(conn, key, value); err != nil {
		return fmt.Errorf("重新SET失败: %w", err)
	}

	// 测试缓存未命中延迟（第一次GET）
	fmt.Printf("   测试缓存未命中延迟...\n")
	var uncachedLatencies []time.Duration
	for i := 0; i < 10; i++ {
		// 删除并重新设置以确保缓存未命中
		sendDelCommand(conn, key)
		time.Sleep(10 * time.Millisecond)
		sendSetCommand(conn, key, value)

		start := time.Now()
		err := sendGetCommand(conn, key)
		latency := time.Since(start)
		if err != nil {
			return fmt.Errorf("GET失败: %w", err)
		}
		uncachedLatencies = append(uncachedLatencies, latency)
		time.Sleep(10 * time.Millisecond)
	}

	// 计算平均延迟
	var cachedTotal, uncachedTotal time.Duration
	for _, lat := range cachedLatencies {
		cachedTotal += lat
	}
	for _, lat := range uncachedLatencies {
		uncachedTotal += lat
	}

	cachedAvg := cachedTotal / time.Duration(len(cachedLatencies))
	uncachedAvg := uncachedTotal / time.Duration(len(uncachedLatencies))

	fmt.Printf("✅ 缓存命中平均延迟: %.3f ms\n", float64(cachedAvg.Nanoseconds())/1000000)
	fmt.Printf("✅ 缓存未命中平均延迟: %.3f ms\n", float64(uncachedAvg.Nanoseconds())/1000000)

	if uncachedAvg > cachedAvg {
		improvement := float64(uncachedAvg-cachedAvg) / float64(uncachedAvg) * 100
		fmt.Printf("🚀 缓存性能提升: %.1f%%\n", improvement)
	} else {
		fmt.Printf("⚠️ 缓存未显示明显性能提升\n")
	}

	return nil
}

// testHotVsColdKey 测试热Key vs 冷Key (冷Key使用nocache:前缀跳过缓存)
func testHotVsColdKey(host, hotKey string) error {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		return fmt.Errorf("连接失败: %w", err)
	}
	defer conn.Close()

	// 创建一个带nocache:前缀的冷Key (不会被缓存)
	coldKey := fmt.Sprintf("nocache:cold_key_%d", time.Now().UnixNano())
	coldValue := "cold_value_no_cache"

	// 设置冷Key
	if err := sendSetCommand(conn, coldKey, coldValue); err != nil {
		return fmt.Errorf("设置冷Key失败: %w", err)
	}

	fmt.Printf("   热Key: %s (会被缓存)\n", hotKey)
	fmt.Printf("   冷Key: %s (nocache:前缀，跳过缓存)\n", coldKey)

	// 测试热Key延迟
	fmt.Printf("   测试热Key延迟...\n")
	var hotLatencies []time.Duration
	for i := 0; i < 20; i++ {
		start := time.Now()
		err := sendGetCommand(conn, hotKey)
		latency := time.Since(start)
		if err != nil {
			return fmt.Errorf("热Key GET失败: %w", err)
		}
		hotLatencies = append(hotLatencies, latency)
		time.Sleep(5 * time.Millisecond)
	}

	// 测试冷Key延迟
	fmt.Printf("   测试冷Key延迟...\n")
	var coldLatencies []time.Duration
	for i := 0; i < 20; i++ {
		start := time.Now()
		err := sendGetCommand(conn, coldKey)
		latency := time.Since(start)
		if err != nil {
			return fmt.Errorf("冷Key GET失败: %w", err)
		}
		coldLatencies = append(coldLatencies, latency)
		time.Sleep(5 * time.Millisecond)
	}

	// 计算统计
	var hotTotal, coldTotal time.Duration
	var hotMin, hotMax, coldMin, coldMax time.Duration = time.Hour, 0, time.Hour, 0

	for _, lat := range hotLatencies {
		hotTotal += lat
		if lat < hotMin {
			hotMin = lat
		}
		if lat > hotMax {
			hotMax = lat
		}
	}

	for _, lat := range coldLatencies {
		coldTotal += lat
		if lat < coldMin {
			coldMin = lat
		}
		if lat > coldMax {
			coldMax = lat
		}
	}

	hotAvg := hotTotal / time.Duration(len(hotLatencies))
	coldAvg := coldTotal / time.Duration(len(coldLatencies))

	fmt.Printf("\n📊 缓存 vs 非缓存延迟对比结果:\n")
	fmt.Printf("热Key统计 (使用缓存):\n")
	fmt.Printf("├── 平均延迟: %.3f ms\n", float64(hotAvg.Nanoseconds())/1000000)
	fmt.Printf("├── 最小延迟: %.3f ms\n", float64(hotMin.Nanoseconds())/1000000)
	fmt.Printf("└── 最大延迟: %.3f ms\n", float64(hotMax.Nanoseconds())/1000000)

	fmt.Printf("冷Key统计 (nocache:前缀，跳过缓存):\n")
	fmt.Printf("├── 平均延迟: %.3f ms\n", float64(coldAvg.Nanoseconds())/1000000)
	fmt.Printf("├── 最小延迟: %.3f ms\n", float64(coldMin.Nanoseconds())/1000000)
	fmt.Printf("└── 最大延迟: %.3f ms\n", float64(coldMax.Nanoseconds())/1000000)

	if coldAvg > hotAvg {
		improvement := float64(coldAvg-hotAvg) / float64(coldAvg) * 100
		fmt.Printf("\n🎯 no_cache_prefix功能效果: 缓存比非缓存快 %.1f%%\n", improvement)
	} else {
		fmt.Printf("\n⚠️ 缓存未显示明显优势，可能需要检查配置\n")
	}

	// 清理冷Key
	sendDelCommand(conn, coldKey)

	return nil
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

// sendDelCommand 发送DEL命令
func sendDelCommand(conn net.Conn, key string) error {
	cmd := fmt.Sprintf("*2\r\n$3\r\nDEL\r\n$%d\r\n%s\r\n", len(key), key)

	if _, err := conn.Write([]byte(cmd)); err != nil {
		return err
	}

	buffer := make([]byte, 1024)
	_, err := conn.Read(buffer)
	return err
}
