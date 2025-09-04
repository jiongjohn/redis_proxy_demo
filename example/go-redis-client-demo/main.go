package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	// 创建Redis客户端连接到代理
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6380", // 连接到代理端口，不是直接Redis端口
		Password: "",               // 无密码
		DB:       0,                // 默认数据库

		// 连接池配置
		PoolSize:        10, // 连接池大小
		MinIdleConns:    2,  // 最小空闲连接数
		MaxIdleConns:    5,  // 最大空闲连接数
		ConnMaxIdleTime: 30 * time.Second,
		ConnMaxLifetime: 10 * time.Minute,
	})

	ctx := context.Background()

	// 测试连接
	fmt.Println("🚀 Go-Redis客户端连接Redis代理演示")
	fmt.Println(strings.Repeat("=", 50))

	// 1. 基础操作测试
	fmt.Println("\n📝 1. 基础 SET/GET 操作测试")
	testBasicOperations(ctx, rdb)

	// 2. 大key操作测试（验证我们修复的bug）
	fmt.Println("\n📦 2. 大Key操作测试 (验证数据完整性)")
	testBigKeyOperations(ctx, rdb)

	// 3. 各种数据类型测试
	fmt.Println("\n🎯 3. 各种数据类型操作测试")
	testDataTypes(ctx, rdb)

	// 4. 事务操作测试
	fmt.Println("\n🔄 4. 事务操作测试 (MULTI/EXEC)")
	testTransactions(ctx, rdb)

	// 5. 管道操作测试
	fmt.Println("\n⚡ 5. 管道操作测试")
	testPipelining(ctx, rdb)

	// 6. 连接信息测试
	fmt.Println("\n🔗 6. 连接信息测试")
	testConnectionInfo(ctx, rdb)

	// 7. 性能测试
	fmt.Println("\n🏆 7. 性能测试")
	testPerformance(ctx, rdb)

	// 关闭客户端
	if err := rdb.Close(); err != nil {
		log.Printf("关闭Redis客户端失败: %v", err)
	}

	fmt.Println("\n✅ 所有测试完成！")
}

// 基础操作测试
func testBasicOperations(ctx context.Context, rdb *redis.Client) {
	// SET操作
	err := rdb.Set(ctx, "demo:hello", "world", 0).Err()
	if err != nil {
		log.Printf("SET失败: %v", err)
		return
	}
	fmt.Println("✅ SET demo:hello world")

	// GET操作
	val, err := rdb.Get(ctx, "demo:hello").Result()
	if err != nil {
		log.Printf("GET失败: %v", err)
		return
	}
	fmt.Printf("✅ GET demo:hello → %s\n", val)

	// 检查key存在
	exists, err := rdb.Exists(ctx, "demo:hello").Result()
	if err != nil {
		log.Printf("EXISTS失败: %v", err)
		return
	}
	fmt.Printf("✅ EXISTS demo:hello → %d\n", exists)

	// 删除key
	deleted, err := rdb.Del(ctx, "demo:hello").Result()
	if err != nil {
		log.Printf("DEL失败: %v", err)
		return
	}
	fmt.Printf("✅ DEL demo:hello → %d\n", deleted)
}

// 大key操作测试
func testBigKeyOperations(ctx context.Context, rdb *redis.Client) {
	// 创建10KB的大数据
	bigData := strings.Repeat("A", 10*1024) // 10KB

	// SET大key
	err := rdb.Set(ctx, "demo:bigkey", bigData, 0).Err()
	if err != nil {
		log.Printf("设置大key失败: %v", err)
		return
	}
	fmt.Printf("✅ SET demo:bigkey (大小: %d 字节)\n", len(bigData))

	// GET大key
	retrievedData, err := rdb.Get(ctx, "demo:bigkey").Result()
	if err != nil {
		log.Printf("获取大key失败: %v", err)
		return
	}

	// 验证数据完整性
	if len(retrievedData) == len(bigData) && retrievedData == bigData {
		fmt.Printf("✅ GET demo:bigkey (大小: %d 字节) - 数据完整性验证通过\n", len(retrievedData))
	} else {
		fmt.Printf("❌ 大key数据不完整! 期望: %d, 实际: %d\n", len(bigData), len(retrievedData))
	}

	// 清理
	rdb.Del(ctx, "demo:bigkey")
}

// 各种数据类型测试
func testDataTypes(ctx context.Context, rdb *redis.Client) {
	// List操作
	rdb.LPush(ctx, "demo:list", "item1", "item2", "item3")
	listLen, _ := rdb.LLen(ctx, "demo:list").Result()
	fmt.Printf("✅ List长度: %d\n", listLen)

	items, _ := rdb.LRange(ctx, "demo:list", 0, -1).Result()
	fmt.Printf("✅ List内容: %v\n", items)

	// Hash操作
	rdb.HSet(ctx, "demo:hash", "field1", "value1", "field2", "value2")
	hashLen, _ := rdb.HLen(ctx, "demo:hash").Result()
	fmt.Printf("✅ Hash字段数: %d\n", hashLen)

	hashData, _ := rdb.HGetAll(ctx, "demo:hash").Result()
	fmt.Printf("✅ Hash内容: %v\n", hashData)

	// Set操作
	rdb.SAdd(ctx, "demo:set", "member1", "member2", "member3")
	setLen, _ := rdb.SCard(ctx, "demo:set").Result()
	fmt.Printf("✅ Set成员数: %d\n", setLen)

	members, _ := rdb.SMembers(ctx, "demo:set").Result()
	fmt.Printf("✅ Set成员: %v\n", members)

	// 清理
	rdb.Del(ctx, "demo:list", "demo:hash", "demo:set")
}

// 事务操作测试
func testTransactions(ctx context.Context, rdb *redis.Client) {
	// 使用MULTI/EXEC事务
	pipe := rdb.TxPipeline()

	pipe.Set(ctx, "demo:tx:key1", "value1", 0)
	pipe.Set(ctx, "demo:tx:key2", "value2", 0)
	pipe.Incr(ctx, "demo:tx:counter")
	pipe.Incr(ctx, "demo:tx:counter")

	cmds, err := pipe.Exec(ctx)
	if err != nil {
		log.Printf("事务执行失败: %v", err)
		return
	}

	fmt.Printf("✅ 事务执行成功，%d个命令\n", len(cmds))

	// 验证结果
	counter, _ := rdb.Get(ctx, "demo:tx:counter").Result()
	fmt.Printf("✅ 计数器值: %s\n", counter)

	// 清理
	rdb.Del(ctx, "demo:tx:key1", "demo:tx:key2", "demo:tx:counter")
}

// 管道操作测试
func testPipelining(ctx context.Context, rdb *redis.Client) {
	pipe := rdb.Pipeline()

	// 批量操作
	for i := 0; i < 5; i++ {
		pipe.Set(ctx, fmt.Sprintf("demo:pipe:key%d", i), fmt.Sprintf("value%d", i), 0)
	}

	cmds, err := pipe.Exec(ctx)
	if err != nil {
		log.Printf("管道执行失败: %v", err)
		return
	}

	fmt.Printf("✅ 管道批量操作成功，%d个命令\n", len(cmds))

	// 批量读取
	pipe = rdb.Pipeline()
	for i := 0; i < 5; i++ {
		pipe.Get(ctx, fmt.Sprintf("demo:pipe:key%d", i))
	}

	cmds, _ = pipe.Exec(ctx)
	for i, cmd := range cmds {
		if getCmd, ok := cmd.(*redis.StringCmd); ok {
			val, _ := getCmd.Result()
			fmt.Printf("✅ Pipe GET key%d → %s\n", i, val)
		}
	}

	// 清理
	for i := 0; i < 5; i++ {
		rdb.Del(ctx, fmt.Sprintf("demo:pipe:key%d", i))
	}
}

// 连接信息测试
func testConnectionInfo(ctx context.Context, rdb *redis.Client) {
	// PING测试
	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Printf("PING失败: %v", err)
		return
	}
	fmt.Printf("✅ PING → %s\n", pong)

	// INFO命令
	info, err := rdb.Info(ctx, "server").Result()
	if err != nil {
		log.Printf("INFO失败: %v", err)
		return
	}

	// 只显示前几行INFO信息
	lines := strings.Split(info, "\n")
	fmt.Println("✅ Redis服务器信息:")
	for i, line := range lines {
		if i >= 3 { // 只显示前3行
			break
		}
		if strings.TrimSpace(line) != "" {
			fmt.Printf("   %s\n", strings.TrimSpace(line))
		}
	}

	// 连接池统计
	stats := rdb.PoolStats()
	fmt.Printf("✅ 连接池统计:\n")
	fmt.Printf("   总连接数: %d\n", stats.TotalConns)
	fmt.Printf("   空闲连接数: %d\n", stats.IdleConns)
	fmt.Printf("   过期连接数: %d\n", stats.StaleConns)
}

// 性能测试
func testPerformance(ctx context.Context, rdb *redis.Client) {
	const numOps = 100

	// SET性能测试
	start := time.Now()
	for i := 0; i < numOps; i++ {
		err := rdb.Set(ctx, fmt.Sprintf("perf:key%d", i), fmt.Sprintf("value%d", i), 0).Err()
		if err != nil {
			log.Printf("性能测试SET失败: %v", err)
			return
		}
	}
	setDuration := time.Since(start)

	// GET性能测试
	start = time.Now()
	for i := 0; i < numOps; i++ {
		_, err := rdb.Get(ctx, fmt.Sprintf("perf:key%d", i)).Result()
		if err != nil {
			log.Printf("性能测试GET失败: %v", err)
			return
		}
	}
	getDuration := time.Since(start)

	fmt.Printf("✅ 性能测试结果 (%d次操作):\n", numOps)
	fmt.Printf("   SET: %v (%.2f ops/sec)\n", setDuration, float64(numOps)/setDuration.Seconds())
	fmt.Printf("   GET: %v (%.2f ops/sec)\n", getDuration, float64(numOps)/getDuration.Seconds())

	// 清理性能测试数据
	for i := 0; i < numOps; i++ {
		rdb.Del(ctx, fmt.Sprintf("perf:key%d", i))
	}
}
