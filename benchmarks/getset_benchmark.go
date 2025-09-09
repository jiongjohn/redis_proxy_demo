package main

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	fmt.Println("🚀 Redis Proxy GET/SET 性能测试")

	// 测试参数
	const (
		host         = "127.0.0.1:6380"
		numClients   = 50
		numRequests  = 1000 // 每个客户端的请求数
		testDuration = 30 * time.Second
	)

	fmt.Printf("测试配置:\n")
	fmt.Printf("- 目标: %s\n", host)
	fmt.Printf("- 并发客户端: %d\n", numClients)
	fmt.Printf("- 每客户端请求数: %d\n", numRequests)
	fmt.Printf("- 测试时长: %v\n", testDuration)
	fmt.Println()

	// 统计变量
	var (
		totalRequests int64
		totalErrors   int64
		getRequests   int64
		setRequests   int64
		totalLatency  int64
		minLatency    int64 = 999999999
		maxLatency    int64
	)

	// 启动时间
	startTime := time.Now()

	// 创建等待组
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
				atomic.AddInt64(&totalErrors, 1)
				return
			}
			defer conn.Close()

			// 执行请求
			for j := 0; j < numRequests; j++ {
				// 交替执行SET和GET
				if j%2 == 0 {
					// SET命令
					key := fmt.Sprintf("key_%d_%d", clientID, j)
					value := fmt.Sprintf("value_%d_%d", clientID, j)

					reqStart := time.Now()
					err := sendSetCommand(conn, key, value)
					latency := time.Since(reqStart).Nanoseconds()

					if err != nil {
						atomic.AddInt64(&totalErrors, 1)
					} else {
						atomic.AddInt64(&setRequests, 1)
						atomic.AddInt64(&totalLatency, latency)

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
				} else {
					// GET命令
					key := fmt.Sprintf("key_%d_%d", clientID, j-1)

					reqStart := time.Now()
					err := sendGetCommand(conn, key)
					latency := time.Since(reqStart).Nanoseconds()

					if err != nil {
						atomic.AddInt64(&totalErrors, 1)
					} else {
						atomic.AddInt64(&getRequests, 1)
						atomic.AddInt64(&totalLatency, latency)

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
				}

				atomic.AddInt64(&totalRequests, 1)
			}
		}(i)
	}

	// 等待所有客户端完成
	wg.Wait()

	// 计算统计信息
	duration := time.Since(startTime)
	totalReqs := atomic.LoadInt64(&totalRequests)
	totalErrs := atomic.LoadInt64(&totalErrors)
	getReqs := atomic.LoadInt64(&getRequests)
	setReqs := atomic.LoadInt64(&setRequests)
	avgLatency := atomic.LoadInt64(&totalLatency) / totalReqs
	minLat := atomic.LoadInt64(&minLatency)
	maxLat := atomic.LoadInt64(&maxLatency)

	// 输出结果
	fmt.Println("\n📊 测试结果:")
	fmt.Printf("- 总请求数: %d\n", totalReqs)
	fmt.Printf("- GET请求数: %d\n", getReqs)
	fmt.Printf("- SET请求数: %d\n", setReqs)
	fmt.Printf("- 错误数: %d\n", totalErrs)
	fmt.Printf("- 成功率: %.2f%%\n", float64(totalReqs-totalErrs)/float64(totalReqs)*100)
	fmt.Printf("- 总耗时: %v\n", duration)
	fmt.Printf("- 吞吐量: %.2f requests/sec\n", float64(totalReqs)/duration.Seconds())
	fmt.Printf("- 平均延迟: %.2f ms\n", float64(avgLatency)/1000000)
	fmt.Printf("- 最小延迟: %.2f ms\n", float64(minLat)/1000000)
	fmt.Printf("- 最大延迟: %.2f ms\n", float64(maxLat)/1000000)
}

// sendSetCommand 发送SET命令
func sendSetCommand(conn net.Conn, key, value string) error {
	// 构建RESP协议命令
	cmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)

	// 发送命令
	_, err := conn.Write([]byte(cmd))
	if err != nil {
		return err
	}

	// 读取响应
	buffer := make([]byte, 1024)
	_, err = conn.Read(buffer)
	return err
}

// sendGetCommand 发送GET命令
func sendGetCommand(conn net.Conn, key string) error {
	// 构建RESP协议命令
	cmd := fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)

	// 发送命令
	_, err := conn.Write([]byte(cmd))
	if err != nil {
		return err
	}

	// 读取响应
	buffer := make([]byte, 1024)
	_, err = conn.Read(buffer)
	return err
}
