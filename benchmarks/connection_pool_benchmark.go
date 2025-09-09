package main

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	fmt.Println("🚀 Redis Proxy 连接池开销对比测试")

	// 测试参数
	const (
		host         = "127.0.0.1:6380"
		numClients   = 50
		numRequests  = 2000 // 每个客户端的请求数
		testDuration = 30 * time.Second
	)

	fmt.Printf("测试配置:\n")
	fmt.Printf("- 目标: %s\n", host)
	fmt.Printf("- 并发客户端: %d\n", numClients)
	fmt.Printf("- 每客户端请求数: %d\n", numRequests)
	fmt.Printf("- 测试时长: %v\n", testDuration)
	fmt.Println()

	// 测试1: GET命令性能
	fmt.Println("====== GET命令性能测试 ======")
	runGetBenchmark(host, numClients, numRequests)

	fmt.Println()

	// 测试2: SET命令性能
	fmt.Println("====== SET命令性能测试 ======")
	runSetBenchmark(host, numClients, numRequests)

	fmt.Println()

	// 测试3: 混合命令性能
	fmt.Println("====== 混合命令性能测试 ======")
	runMixedBenchmark(host, numClients, numRequests)
}

// runGetBenchmark 运行GET命令基准测试
func runGetBenchmark(host string, numClients, numRequests int) {
	var (
		totalRequests int64
		totalErrors   int64
		totalLatency  int64
		minLatency    int64 = 999999999
		maxLatency    int64
	)

	startTime := time.Now()
	var wg sync.WaitGroup

	// 启动客户端
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			// 建立连接
			conn, err := net.Dial("tcp", host)
			if err != nil {
				fmt.Printf("客户端 %d 连接失败: %v\n", clientID, err)
				return
			}
			defer conn.Close()

			// 执行GET请求
			for j := 0; j < numRequests; j++ {
				requestStart := time.Now()

				// 发送GET命令
				key := fmt.Sprintf("test_key_%d_%d", clientID, j)
				command := fmt.Sprintf("GET %s\r\n", key)

				_, err := conn.Write([]byte(command))
				if err != nil {
					atomic.AddInt64(&totalErrors, 1)
					continue
				}

				// 读取响应
				buffer := make([]byte, 1024)
				_, err = conn.Read(buffer)
				if err != nil {
					atomic.AddInt64(&totalErrors, 1)
					continue
				}

				// 计算延迟
				latency := time.Since(requestStart).Nanoseconds()
				atomic.AddInt64(&totalLatency, latency)
				atomic.AddInt64(&totalRequests, 1)

				// 更新最小/最大延迟
				for {
					current := atomic.LoadInt64(&minLatency)
					if latency >= current || atomic.CompareAndSwapInt64(&minLatency, current, latency) {
						break
					}
				}
				for {
					current := atomic.LoadInt64(&maxLatency)
					if latency <= current || atomic.CompareAndSwapInt64(&maxLatency, current, latency) {
						break
					}
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	// 计算统计信息
	requests := atomic.LoadInt64(&totalRequests)
	errors := atomic.LoadInt64(&totalErrors)
	avgLatency := atomic.LoadInt64(&totalLatency) / requests

	fmt.Printf("GET命令测试结果:\n")
	fmt.Printf("- 总请求数: %d\n", requests)
	fmt.Printf("- 错误数: %d\n", errors)
	fmt.Printf("- 成功率: %.2f%%\n", float64(requests-errors)/float64(requests)*100)
	fmt.Printf("- 总耗时: %v\n", duration)
	fmt.Printf("- 吞吐量: %.2f requests/sec\n", float64(requests)/duration.Seconds())
	fmt.Printf("- 平均延迟: %.2f ms\n", float64(avgLatency)/1e6)
	fmt.Printf("- 最小延迟: %.2f ms\n", float64(atomic.LoadInt64(&minLatency))/1e6)
	fmt.Printf("- 最大延迟: %.2f ms\n", float64(atomic.LoadInt64(&maxLatency))/1e6)
}

// runSetBenchmark 运行SET命令基准测试
func runSetBenchmark(host string, numClients, numRequests int) {
	var (
		totalRequests int64
		totalErrors   int64
		totalLatency  int64
		minLatency    int64 = 999999999
		maxLatency    int64
	)

	startTime := time.Now()
	var wg sync.WaitGroup

	// 启动客户端
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			// 建立连接
			conn, err := net.Dial("tcp", host)
			if err != nil {
				fmt.Printf("客户端 %d 连接失败: %v\n", clientID, err)
				return
			}
			defer conn.Close()

			// 执行SET请求
			for j := 0; j < numRequests; j++ {
				requestStart := time.Now()

				// 发送SET命令
				key := fmt.Sprintf("test_key_%d_%d", clientID, j)
				value := fmt.Sprintf("test_value_%d_%d_%d", clientID, j, time.Now().UnixNano())
				command := fmt.Sprintf("SET %s %s\r\n", key, value)

				_, err := conn.Write([]byte(command))
				if err != nil {
					atomic.AddInt64(&totalErrors, 1)
					continue
				}

				// 读取响应
				buffer := make([]byte, 1024)
				_, err = conn.Read(buffer)
				if err != nil {
					atomic.AddInt64(&totalErrors, 1)
					continue
				}

				// 计算延迟
				latency := time.Since(requestStart).Nanoseconds()
				atomic.AddInt64(&totalLatency, latency)
				atomic.AddInt64(&totalRequests, 1)

				// 更新最小/最大延迟
				for {
					current := atomic.LoadInt64(&minLatency)
					if latency >= current || atomic.CompareAndSwapInt64(&minLatency, current, latency) {
						break
					}
				}
				for {
					current := atomic.LoadInt64(&maxLatency)
					if latency <= current || atomic.CompareAndSwapInt64(&maxLatency, current, latency) {
						break
					}
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	// 计算统计信息
	requests := atomic.LoadInt64(&totalRequests)
	errors := atomic.LoadInt64(&totalErrors)
	avgLatency := atomic.LoadInt64(&totalLatency) / requests

	fmt.Printf("SET命令测试结果:\n")
	fmt.Printf("- 总请求数: %d\n", requests)
	fmt.Printf("- 错误数: %d\n", errors)
	fmt.Printf("- 成功率: %.2f%%\n", float64(requests-errors)/float64(requests)*100)
	fmt.Printf("- 总耗时: %v\n", duration)
	fmt.Printf("- 吞吐量: %.2f requests/sec\n", float64(requests)/duration.Seconds())
	fmt.Printf("- 平均延迟: %.2f ms\n", float64(avgLatency)/1e6)
	fmt.Printf("- 最小延迟: %.2f ms\n", float64(atomic.LoadInt64(&minLatency))/1e6)
	fmt.Printf("- 最大延迟: %.2f ms\n", float64(atomic.LoadInt64(&maxLatency))/1e6)
}

// runMixedBenchmark 运行混合命令基准测试
func runMixedBenchmark(host string, numClients, numRequests int) {
	var (
		totalRequests int64
		totalErrors   int64
		getRequests   int64
		setRequests   int64
		totalLatency  int64
		minLatency    int64 = 999999999
		maxLatency    int64
	)

	startTime := time.Now()
	var wg sync.WaitGroup

	// 启动客户端
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			// 建立连接
			conn, err := net.Dial("tcp", host)
			if err != nil {
				fmt.Printf("客户端 %d 连接失败: %v\n", clientID, err)
				return
			}
			defer conn.Close()

			// 执行混合请求（70% GET, 30% SET）
			for j := 0; j < numRequests; j++ {
				requestStart := time.Now()

				var command string
				if j%10 < 7 { // 70% GET
					key := fmt.Sprintf("test_key_%d_%d", clientID, j%100) // 复用key增加命中率
					command = fmt.Sprintf("GET %s\r\n", key)
					atomic.AddInt64(&getRequests, 1)
				} else { // 30% SET
					key := fmt.Sprintf("test_key_%d_%d", clientID, j%100)
					value := fmt.Sprintf("value_%d_%d", clientID, j)
					command = fmt.Sprintf("SET %s %s\r\n", key, value)
					atomic.AddInt64(&setRequests, 1)
				}

				_, err := conn.Write([]byte(command))
				if err != nil {
					atomic.AddInt64(&totalErrors, 1)
					continue
				}

				// 读取响应
				buffer := make([]byte, 1024)
				_, err = conn.Read(buffer)
				if err != nil {
					atomic.AddInt64(&totalErrors, 1)
					continue
				}

				// 计算延迟
				latency := time.Since(requestStart).Nanoseconds()
				atomic.AddInt64(&totalLatency, latency)
				atomic.AddInt64(&totalRequests, 1)

				// 更新最小/最大延迟
				for {
					current := atomic.LoadInt64(&minLatency)
					if latency >= current || atomic.CompareAndSwapInt64(&minLatency, current, latency) {
						break
					}
				}
				for {
					current := atomic.LoadInt64(&maxLatency)
					if latency <= current || atomic.CompareAndSwapInt64(&maxLatency, current, latency) {
						break
					}
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	// 计算统计信息
	requests := atomic.LoadInt64(&totalRequests)
	errors := atomic.LoadInt64(&totalErrors)
	gets := atomic.LoadInt64(&getRequests)
	sets := atomic.LoadInt64(&setRequests)
	avgLatency := atomic.LoadInt64(&totalLatency) / requests

	fmt.Printf("混合命令测试结果:\n")
	fmt.Printf("- 总请求数: %d (GET: %d, SET: %d)\n", requests, gets, sets)
	fmt.Printf("- 错误数: %d\n", errors)
	fmt.Printf("- 成功率: %.2f%%\n", float64(requests-errors)/float64(requests)*100)
	fmt.Printf("- 总耗时: %v\n", duration)
	fmt.Printf("- 吞吐量: %.2f requests/sec\n", float64(requests)/duration.Seconds())
	fmt.Printf("- 平均延迟: %.2f ms\n", float64(avgLatency)/1e6)
	fmt.Printf("- 最小延迟: %.2f ms\n", float64(atomic.LoadInt64(&minLatency))/1e6)
	fmt.Printf("- 最大延迟: %.2f ms\n", float64(atomic.LoadInt64(&maxLatency))/1e6)
}
