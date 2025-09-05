package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func simpleTest() {
	fmt.Println("🧪 简单 Go-Redis 客户端测试")
	fmt.Println(strings.Repeat("=", 40))

	// 创建客户端，使用更短的超时时间
	rdb := redis.NewClient(&redis.Options{
		Addr:         "localhost:6380",
		Password:     "",
		DB:           0,
		DialTimeout:  5 * time.Second, // 连接超时
		ReadTimeout:  5 * time.Second, // 读取超时
		WriteTimeout: 5 * time.Second, // 写入超时
		PoolTimeout:  5 * time.Second, // 池超时

		// 简化连接池配置
		PoolSize:     1, // 只用一个连接
		MinIdleConns: 1,
		MaxIdleConns: 1,
	})
	fmt.Printf("rdb success %v", rdb)

	ctx := context.Background()

	fmt.Println("\n🔍 1. 基础连接测试")
	var err error
	// 1. PING 测试
	fmt.Print("   PING 测试... ")
	//pong, err := rdb.Ping(ctx).Result()
	//if err != nil {
	//	fmt.Printf("❌ 失败: %v\n", err)
	//} else {
	//	fmt.Printf("✅ 成功: %s\n", pong)
	//}
	//
	//fmt.Println("\n📝 2. 基础 SET/GET 测试")
	//
	// 2. SET 测试
	fmt.Print("   SET test:key hello... ")
	err = rdb.Set(ctx, "test:key", "hello", 0).Err()
	if err != nil {
		fmt.Printf("❌ 失败: %v\n", err)
	} else {
		fmt.Printf("✅ 成功\n")
	}

	// 3. GET 测试
	fmt.Print("   GET test:key... ")
	val, err := rdb.Get(ctx, "test:key").Result()
	if err != nil {
		fmt.Printf("❌ 失败: %v\n", err)
	} else {
		fmt.Printf("✅ 成功: %s\n", val)
	}

	fmt.Println("\n📦 3. 中等大小数据测试")

	// 4. 中等大小数据测试 (1KB)
	testData := strings.Repeat("X", 1024)
	fmt.Printf("   SET test:medium 1KB数据... ")
	err = rdb.Set(ctx, "test:medium", testData, 0).Err()
	if err != nil {
		fmt.Printf("❌ 失败: %v\n", err)
	} else {
		fmt.Printf("✅ 成功\n")
	}

	fmt.Print("   GET test:medium... ")
	result, err := rdb.Get(ctx, "test:medium").Result()
	if err != nil {
		fmt.Printf("❌ 失败: %v\n", err)
	} else {
		if len(result) == len(testData) && result == testData {
			fmt.Printf("✅ 成功: 数据完整 (%d 字节)\n", len(result))
		} else {
			fmt.Printf("❌ 数据不完整: 期望 %d, 实际 %d 字节\n", len(testData), len(result))
		}
	}

	fmt.Println("\n🧹 4. 清理测试")

	// 5. 清理测试数据
	fmt.Print("   清理测试数据... ")
	deleted := rdb.Del(ctx, "test:key", "test:medium").Val()
	fmt.Printf("✅ 删除了 %d 个key\n", deleted)

	// 关闭连接
	err = rdb.Close()
	if err != nil {
		fmt.Printf("关闭客户端失败: %v\n", err)
	}

	fmt.Println("\n🎉 简单测试完成!")
}

func main() {
	simpleTest()
}
