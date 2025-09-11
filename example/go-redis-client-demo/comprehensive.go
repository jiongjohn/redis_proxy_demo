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
		Addr:     "localhost:6380", // 连接到代理端口
		Password: "",               // 无密码
		DB:       0,                // 默认数据库

		// 连接池配置
		PoolSize:        20, // 连接池大小
		MinIdleConns:    5,  // 最小空闲连接数
		MaxIdleConns:    10, // 最大空闲连接数
		ConnMaxIdleTime: 30 * time.Second,
		ConnMaxLifetime: 10 * time.Minute,

		// 超时配置
		DialTimeout:  5 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	})

	ctx := context.Background()

	// 测试连接
	fmt.Println("🚀 Redis 代理全面命令测试")
	fmt.Println(strings.Repeat("=", 60))

	// 清理测试数据
	fmt.Println("\n🧹 清理旧测试数据...")
	cleanupTestData(ctx, rdb)

	// 1. 字符串操作测试
	fmt.Println("\n📝 1. 字符串(String)操作测试")
	testStringOperations(ctx, rdb)

	// 2. 哈希操作测试
	fmt.Println("\n🗂️  2. 哈希(Hash)操作测试")
	testHashOperations(ctx, rdb)

	// 3. 列表操作测试
	fmt.Println("\n📋 3. 列表(List)操作测试")
	testListOperations(ctx, rdb)

	// 4. 集合操作测试
	fmt.Println("\n🎯 4. 集合(Set)操作测试")
	testSetOperations(ctx, rdb)

	// 5. 有序集合操作测试
	fmt.Println("\n📊 5. 有序集合(Sorted Set)操作测试")
	testSortedSetOperations(ctx, rdb)

	// 6. 位图操作测试
	fmt.Println("\n🔢 6. 位图(Bitmap)操作测试")
	testBitmapOperations(ctx, rdb)

	// 7. 过期和TTL测试
	fmt.Println("\n⏰ 7. 过期和TTL测试")
	testExpirationOperations(ctx, rdb)

	// 8. 事务操作测试
	fmt.Println("\n🔄 8. 事务(Transaction)操作测试")
	testTransactionOperations(ctx, rdb)

	// 9. 管道操作测试
	fmt.Println("\n⚡ 9. 管道(Pipeline)操作测试")
	testPipelineOperations(ctx, rdb)

	// 10. 原子操作测试
	fmt.Println("\n⚛️  10. 原子操作测试")
	testAtomicOperations(ctx, rdb)

	// 11. 大数据测试
	fmt.Println("\n📦 11. 大数据操作测试")
	testBigDataOperations(ctx, rdb)

	// 12. 并发测试
	fmt.Println("\n🏃 12. 并发操作测试")
	testConcurrentOperations(ctx, rdb)

	// 13. 连接信息测试
	fmt.Println("\n🔗 13. 连接信息测试")
	testConnectionInfoV2(ctx, rdb)

	// 最终清理
	fmt.Println("\n🧹 最终清理测试数据...")
	cleanupTestData(ctx, rdb)

	// 关闭客户端
	if err := rdb.Close(); err != nil {
		log.Printf("关闭Redis客户端失败: %v", err)
	}

	fmt.Println("\n✅ 所有测试完成！")
}

// 字符串操作测试
func testStringOperations(ctx context.Context, rdb *redis.Client) {
	fmt.Println("   测试 SET/GET/SETNX/GETSET/INCR/DECR...")

	// SET 和 GET
	err := rdb.Set(ctx, "test:str:basic", "hello world", 0).Err()
	checkError("SET", err)

	val, err := rdb.Get(ctx, "test:str:basic").Result()
	checkError("GET", err)
	fmt.Printf("   ✅ GET test:str:basic → %s\n", val)

	// SETNX (SET if Not eXists)
	success, err := rdb.SetNX(ctx, "test:str:nx", "new value", 0).Result()
	checkError("SETNX", err)
	fmt.Printf("   ✅ SETNX test:str:nx → %t\n", success)

	// 再次SETNX应该失败
	success, err = rdb.SetNX(ctx, "test:str:nx", "another value", 0).Result()
	checkError("SETNX", err)
	fmt.Printf("   ✅ SETNX test:str:nx (再次) → %t\n", success)

	// GETSET
	oldVal, err := rdb.GetSet(ctx, "test:str:basic", "new hello").Result()
	checkError("GETSET", err)
	fmt.Printf("   ✅ GETSET test:str:basic → 旧值: %s\n", oldVal)

	// INCR 和 DECR
	rdb.Set(ctx, "test:str:counter", "10", 0)
	newVal, err := rdb.Incr(ctx, "test:str:counter").Result()
	checkError("INCR", err)
	fmt.Printf("   ✅ INCR test:str:counter → %d\n", newVal)

	newVal, err = rdb.Decr(ctx, "test:str:counter").Result()
	checkError("DECR", err)
	fmt.Printf("   ✅ DECR test:str:counter → %d\n", newVal)

	// INCRBY 和 DECRBY
	newVal, err = rdb.IncrBy(ctx, "test:str:counter", 5).Result()
	checkError("INCRBY", err)
	fmt.Printf("   ✅ INCRBY test:str:counter 5 → %d\n", newVal)

	newVal, err = rdb.DecrBy(ctx, "test:str:counter", 3).Result()
	checkError("DECRBY", err)
	fmt.Printf("   ✅ DECRBY test:str:counter 3 → %d\n", newVal)

	// APPEND
	length, err := rdb.Append(ctx, "test:str:basic", " appended").Result()
	checkError("APPEND", err)
	fmt.Printf("   ✅ APPEND test:str:basic → 新长度: %d\n", length)

	// STRLEN
	length, err = rdb.StrLen(ctx, "test:str:basic").Result()
	checkError("STRLEN", err)
	fmt.Printf("   ✅ STRLEN test:str:basic → %d\n", length)
}

// 哈希操作测试
func testHashOperations(ctx context.Context, rdb *redis.Client) {
	fmt.Println("   测试 HSET/HGET/HMSET/HMGET/HGETALL/HDEL...")

	// HSET 和 HGET
	err := rdb.HSet(ctx, "test:hash:user", "name", "张三", "age", "25", "city", "北京").Err()
	checkError("HSET", err)

	name, err := rdb.HGet(ctx, "test:hash:user", "name").Result()
	checkError("HGET", err)
	fmt.Printf("   ✅ HGET test:hash:user name → %s\n", name)

	// HMGET (批量获取)
	values, err := rdb.HMGet(ctx, "test:hash:user", "name", "age", "city").Result()
	checkError("HMGET", err)
	fmt.Printf("   ✅ HMGET test:hash:user → %v\n", values)

	// HGETALL
	allFields, err := rdb.HGetAll(ctx, "test:hash:user").Result()
	checkError("HGETALL", err)
	fmt.Printf("   ✅ HGETALL test:hash:user → %v\n", allFields)

	// HLEN
	length, err := rdb.HLen(ctx, "test:hash:user").Result()
	checkError("HLEN", err)
	fmt.Printf("   ✅ HLEN test:hash:user → %d\n", length)

	// HEXISTS
	exists, err := rdb.HExists(ctx, "test:hash:user", "name").Result()
	checkError("HEXISTS", err)
	fmt.Printf("   ✅ HEXISTS test:hash:user name → %t\n", exists)

	// HKEYS 和 HVALS
	keys, err := rdb.HKeys(ctx, "test:hash:user").Result()
	checkError("HKEYS", err)
	fmt.Printf("   ✅ HKEYS test:hash:user → %v\n", keys)

	vals, err := rdb.HVals(ctx, "test:hash:user").Result()
	checkError("HVALS", err)
	fmt.Printf("   ✅ HVALS test:hash:user → %v\n", vals)

	// HINCRBY
	rdb.HSet(ctx, "test:hash:counter", "score", "100")
	newScore, err := rdb.HIncrBy(ctx, "test:hash:counter", "score", 10).Result()
	checkError("HINCRBY", err)
	fmt.Printf("   ✅ HINCRBY test:hash:counter score 10 → %d\n", newScore)

	// HDEL
	deleted, err := rdb.HDel(ctx, "test:hash:user", "city").Result()
	checkError("HDEL", err)
	fmt.Printf("   ✅ HDEL test:hash:user city → %d\n", deleted)
}

// 列表操作测试
func testListOperations(ctx context.Context, rdb *redis.Client) {
	fmt.Println("   测试 LPUSH/RPUSH/LPOP/RPOP/LRANGE/LLEN...")

	// LPUSH 和 RPUSH
	err := rdb.LPush(ctx, "test:list:queue", "item1", "item2", "item3").Err()
	checkError("LPUSH", err)

	err = rdb.RPush(ctx, "test:list:queue", "item4", "item5").Err()
	checkError("RPUSH", err)

	// LLEN
	length, err := rdb.LLen(ctx, "test:list:queue").Result()
	checkError("LLEN", err)
	fmt.Printf("   ✅ LLEN test:list:queue → %d\n", length)

	// LRANGE
	items, err := rdb.LRange(ctx, "test:list:queue", 0, -1).Result()
	checkError("LRANGE", err)
	fmt.Printf("   ✅ LRANGE test:list:queue 0 -1 → %v\n", items)

	// LPOP 和 RPOP
	leftItem, err := rdb.LPop(ctx, "test:list:queue").Result()
	checkError("LPOP", err)
	fmt.Printf("   ✅ LPOP test:list:queue → %s\n", leftItem)

	rightItem, err := rdb.RPop(ctx, "test:list:queue").Result()
	checkError("RPOP", err)
	fmt.Printf("   ✅ RPOP test:list:queue → %s\n", rightItem)

	// LINDEX
	item, err := rdb.LIndex(ctx, "test:list:queue", 0).Result()
	checkError("LINDEX", err)
	fmt.Printf("   ✅ LINDEX test:list:queue 0 → %s\n", item)

	// LSET
	err = rdb.LSet(ctx, "test:list:queue", 0, "modified_item").Err()
	checkError("LSET", err)

	// LTRIM
	err = rdb.LTrim(ctx, "test:list:queue", 0, 2).Err()
	checkError("LTRIM", err)

	// 查看修剪后的列表
	items, _ = rdb.LRange(ctx, "test:list:queue", 0, -1).Result()
	fmt.Printf("   ✅ LTRIM 后的列表 → %v\n", items)
}

// 集合操作测试
func testSetOperations(ctx context.Context, rdb *redis.Client) {
	fmt.Println("   测试 SADD/SMEMBERS/SCARD/SISMEMBER/SPOP/SREM...")

	// SADD
	err := rdb.SAdd(ctx, "test:set:tags", "redis", "database", "cache", "nosql").Err()
	checkError("SADD", err)

	// SCARD
	count, err := rdb.SCard(ctx, "test:set:tags").Result()
	checkError("SCARD", err)
	fmt.Printf("   ✅ SCARD test:set:tags → %d\n", count)

	// SMEMBERS
	members, err := rdb.SMembers(ctx, "test:set:tags").Result()
	checkError("SMEMBERS", err)
	fmt.Printf("   ✅ SMEMBERS test:set:tags → %v\n", members)

	// SISMEMBER
	isMember, err := rdb.SIsMember(ctx, "test:set:tags", "redis").Result()
	checkError("SISMEMBER", err)
	fmt.Printf("   ✅ SISMEMBER test:set:tags redis → %t\n", isMember)

	// SPOP
	poppedMember, err := rdb.SPop(ctx, "test:set:tags").Result()
	checkError("SPOP", err)
	fmt.Printf("   ✅ SPOP test:set:tags → %s\n", poppedMember)

	// SRANDMEMBER
	randomMember, err := rdb.SRandMember(ctx, "test:set:tags").Result()
	checkError("SRANDMEMBER", err)
	fmt.Printf("   ✅ SRANDMEMBER test:set:tags → %s\n", randomMember)

	// 创建另一个集合用于集合运算
	rdb.SAdd(ctx, "test:set:tags2", "redis", "mongodb", "mysql")

	// SINTER (交集)
	intersection, err := rdb.SInter(ctx, "test:set:tags", "test:set:tags2").Result()
	checkError("SINTER", err)
	fmt.Printf("   ✅ SINTER test:set:tags test:set:tags2 → %v\n", intersection)

	// SUNION (并集)
	union, err := rdb.SUnion(ctx, "test:set:tags", "test:set:tags2").Result()
	checkError("SUNION", err)
	fmt.Printf("   ✅ SUNION test:set:tags test:set:tags2 → %v\n", union)

	// SDIFF (差集)
	diff, err := rdb.SDiff(ctx, "test:set:tags", "test:set:tags2").Result()
	checkError("SDIFF", err)
	fmt.Printf("   ✅ SDIFF test:set:tags test:set:tags2 → %v\n", diff)

	// SREM
	removed, err := rdb.SRem(ctx, "test:set:tags", "cache").Result()
	checkError("SREM", err)
	fmt.Printf("   ✅ SREM test:set:tags cache → %d\n", removed)
}

// 有序集合操作测试
func testSortedSetOperations(ctx context.Context, rdb *redis.Client) {
	fmt.Println("   测试 ZADD/ZRANGE/ZCARD/ZSCORE/ZRANK...")

	// ZADD
	err := rdb.ZAdd(ctx, "test:zset:scores",
		redis.Z{Score: 100, Member: "alice"},
		redis.Z{Score: 85, Member: "bob"},
		redis.Z{Score: 92, Member: "charlie"},
		redis.Z{Score: 78, Member: "david"},
	).Err()
	checkError("ZADD", err)

	// ZCARD
	count, err := rdb.ZCard(ctx, "test:zset:scores").Result()
	checkError("ZCARD", err)
	fmt.Printf("   ✅ ZCARD test:zset:scores → %d\n", count)

	// ZRANGE (按分数排序)
	members, err := rdb.ZRange(ctx, "test:zset:scores", 0, -1).Result()
	checkError("ZRANGE", err)
	fmt.Printf("   ✅ ZRANGE test:zset:scores 0 -1 → %v\n", members)

	// ZREVRANGE (按分数倒序)
	revMembers, err := rdb.ZRevRange(ctx, "test:zset:scores", 0, -1).Result()
	checkError("ZREVRANGE", err)
	fmt.Printf("   ✅ ZREVRANGE test:zset:scores 0 -1 → %v\n", revMembers)

	// ZSCORE
	score, err := rdb.ZScore(ctx, "test:zset:scores", "alice").Result()
	checkError("ZSCORE", err)
	fmt.Printf("   ✅ ZSCORE test:zset:scores alice → %.0f\n", score)

	// ZRANK
	rank, err := rdb.ZRank(ctx, "test:zset:scores", "alice").Result()
	checkError("ZRANK", err)
	fmt.Printf("   ✅ ZRANK test:zset:scores alice → %d\n", rank)

	// ZREVRANK
	revRank, err := rdb.ZRevRank(ctx, "test:zset:scores", "alice").Result()
	checkError("ZREVRANK", err)
	fmt.Printf("   ✅ ZREVRANK test:zset:scores alice → %d\n", revRank)

	// ZINCRBY
	newScore, err := rdb.ZIncrBy(ctx, "test:zset:scores", 5, "bob").Result()
	checkError("ZINCRBY", err)
	fmt.Printf("   ✅ ZINCRBY test:zset:scores 5 bob → %.0f\n", newScore)

	// ZRANGEBYSCORE
	scoreMembers, err := rdb.ZRangeByScore(ctx, "test:zset:scores", &redis.ZRangeBy{
		Min: "80",
		Max: "95",
	}).Result()
	checkError("ZRANGEBYSCORE", err)
	fmt.Printf("   ✅ ZRANGEBYSCORE test:zset:scores 80 95 → %v\n", scoreMembers)

	// ZREM
	removed, err := rdb.ZRem(ctx, "test:zset:scores", "david").Result()
	checkError("ZREM", err)
	fmt.Printf("   ✅ ZREM test:zset:scores david → %d\n", removed)
}

// 位图操作测试
func testBitmapOperations(ctx context.Context, rdb *redis.Client) {
	fmt.Println("   测试 SETBIT/GETBIT/BITCOUNT...")

	// SETBIT
	err := rdb.SetBit(ctx, "test:bitmap:users", 100, 1).Err()
	checkError("SETBIT", err)

	err = rdb.SetBit(ctx, "test:bitmap:users", 200, 1).Err()
	checkError("SETBIT", err)

	err = rdb.SetBit(ctx, "test:bitmap:users", 300, 1).Err()
	checkError("SETBIT", err)

	// GETBIT
	bit, err := rdb.GetBit(ctx, "test:bitmap:users", 100).Result()
	checkError("GETBIT", err)
	fmt.Printf("   ✅ GETBIT test:bitmap:users 100 → %d\n", bit)

	bit, err = rdb.GetBit(ctx, "test:bitmap:users", 150).Result()
	checkError("GETBIT", err)
	fmt.Printf("   ✅ GETBIT test:bitmap:users 150 → %d\n", bit)

	// BITCOUNT
	count, err := rdb.BitCount(ctx, "test:bitmap:users", &redis.BitCount{}).Result()
	checkError("BITCOUNT", err)
	fmt.Printf("   ✅ BITCOUNT test:bitmap:users → %d\n", count)
}

// 过期和TTL测试
func testExpirationOperations(ctx context.Context, rdb *redis.Client) {
	fmt.Println("   测试 EXPIRE/TTL/PERSIST...")

	// 设置一个key
	rdb.Set(ctx, "test:expire:key", "will expire", 0)

	// EXPIRE
	err := rdb.Expire(ctx, "test:expire:key", 10*time.Second).Err()
	checkError("EXPIRE", err)

	// TTL
	ttl, err := rdb.TTL(ctx, "test:expire:key").Result()
	checkError("TTL", err)
	fmt.Printf("   ✅ TTL test:expire:key → %v\n", ttl)

	// PERSIST (移除过期时间)
	err = rdb.Persist(ctx, "test:expire:key").Err()
	checkError("PERSIST", err)

	// 再次检查TTL
	ttl, err = rdb.TTL(ctx, "test:expire:key").Result()
	checkError("TTL", err)
	fmt.Printf("   ✅ TTL test:expire:key (after PERSIST) → %v\n", ttl)

	// SETEX (设置key并指定过期时间)
	err = rdb.SetEx(ctx, "test:setex:key", "expires in 5 seconds", 5*time.Second).Err()
	checkError("SETEX", err)

	ttl, _ = rdb.TTL(ctx, "test:setex:key").Result()
	fmt.Printf("   ✅ SETEX test:setex:key → TTL: %v\n", ttl)
}

// 事务操作测试
func testTransactionOperations(ctx context.Context, rdb *redis.Client) {
	fmt.Println("   测试 MULTI/EXEC 事务...")

	// 使用TxPipeline进行事务操作
	pipe := rdb.TxPipeline()

	pipe.Set(ctx, "test:tx:key1", "value1", 0)
	pipe.Set(ctx, "test:tx:key2", "value2", 0)
	pipe.Incr(ctx, "test:tx:counter")
	pipe.Incr(ctx, "test:tx:counter")
	pipe.Get(ctx, "test:tx:key1")

	cmds, err := pipe.Exec(ctx)
	checkError("MULTI/EXEC", err)

	fmt.Printf("   ✅ 事务执行成功，%d个命令\n", len(cmds))

	// 验证结果
	counter, _ := rdb.Get(ctx, "test:tx:counter").Result()
	fmt.Printf("   ✅ 事务后计数器值: %s\n", counter)

	// 获取最后一个GET命令的结果
	if len(cmds) > 0 {
		if getCmd, ok := cmds[len(cmds)-1].(*redis.StringCmd); ok {
			val, _ := getCmd.Result()
			fmt.Printf("   ✅ 事务中GET结果: %s\n", val)
		}
	}
}

// 管道操作测试
func testPipelineOperations(ctx context.Context, rdb *redis.Client) {
	fmt.Println("   测试 Pipeline 批量操作...")

	pipe := rdb.Pipeline()

	// 批量SET操作
	for i := 0; i < 10; i++ {
		pipe.Set(ctx, fmt.Sprintf("test:pipe:key%d", i), fmt.Sprintf("value%d", i), 0)
	}

	cmds, err := pipe.Exec(ctx)
	checkError("Pipeline SET", err)
	fmt.Printf("   ✅ Pipeline批量SET成功，%d个命令\n", len(cmds))

	// 批量GET操作
	pipe = rdb.Pipeline()
	for i := 0; i < 10; i++ {
		pipe.Get(ctx, fmt.Sprintf("test:pipe:key%d", i))
	}

	cmds, err = pipe.Exec(ctx)
	checkError("Pipeline GET", err)

	fmt.Printf("   ✅ Pipeline批量GET成功:\n")
	for i, cmd := range cmds {
		if getCmd, ok := cmd.(*redis.StringCmd); ok {
			val, _ := getCmd.Result()
			fmt.Printf("      key%d → %s\n", i, val)
		}
	}
}

// 原子操作测试
func testAtomicOperations(ctx context.Context, rdb *redis.Client) {
	fmt.Println("   测试原子操作...")

	// 初始化计数器
	rdb.Set(ctx, "test:atomic:counter", "0", 0)

	// INCR 原子递增
	for i := 0; i < 5; i++ {
		val, err := rdb.Incr(ctx, "test:atomic:counter").Result()
		checkError("INCR", err)
		fmt.Printf("   ✅ INCR #%d → %d\n", i+1, val)
	}

	// INCRBY 原子增加指定值
	val, err := rdb.IncrBy(ctx, "test:atomic:counter", 10).Result()
	checkError("INCRBY", err)
	fmt.Printf("   ✅ INCRBY 10 → %d\n", val)

	// 测试SETNX的原子性
	success1, _ := rdb.SetNX(ctx, "test:atomic:lock", "process1", 5*time.Second).Result()
	success2, _ := rdb.SetNX(ctx, "test:atomic:lock", "process2", 5*time.Second).Result()

	fmt.Printf("   ✅ SETNX 原子锁测试: process1=%t, process2=%t\n", success1, success2)
}

// 大数据操作测试
func testBigDataOperations(ctx context.Context, rdb *redis.Client) {
	fmt.Println("   测试大数据操作...")

	// 测试不同大小的数据
	sizes := []int{1024, 10240, 102400} // 1KB, 10KB, 100KB

	for _, size := range sizes {
		data := strings.Repeat("X", size)
		key := fmt.Sprintf("test:bigdata:%d", size)

		// SET大数据
		start := time.Now()
		err := rdb.Set(ctx, key, data, 0).Err()
		setDuration := time.Since(start)
		checkError(fmt.Sprintf("SET %dB", size), err)

		// GET大数据
		start = time.Now()
		result, err := rdb.Get(ctx, key).Result()
		getDuration := time.Since(start)
		checkError(fmt.Sprintf("GET %dB", size), err)

		// 验证数据完整性
		if len(result) == len(data) && result == data {
			fmt.Printf("   ✅ %dB 数据: SET %v, GET %v, 完整性✓\n",
				size, setDuration, getDuration)
		} else {
			fmt.Printf("   ❌ %dB 数据完整性检查失败\n", size)
		}
	}
}

// 并发操作测试
func testConcurrentOperations(ctx context.Context, rdb *redis.Client) {
	fmt.Println("   测试并发操作...")

	const numGoroutines = 10
	const numOpsPerGoroutine = 10

	// 并发SET操作
	start := time.Now()
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < numOpsPerGoroutine; j++ {
				key := fmt.Sprintf("test:concurrent:g%d:k%d", id, j)
				value := fmt.Sprintf("value_%d_%d", id, j)
				rdb.Set(ctx, key, value, 0)
			}
			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	duration := time.Since(start)
	totalOps := numGoroutines * numOpsPerGoroutine

	fmt.Printf("   ✅ 并发SET: %d个操作, 耗时 %v, 速率 %.2f ops/sec\n",
		totalOps, duration, float64(totalOps)/duration.Seconds())

	// 验证数据
	correctCount := 0
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < numOpsPerGoroutine; j++ {
			key := fmt.Sprintf("test:concurrent:g%d:k%d", i, j)
			expectedValue := fmt.Sprintf("value_%d_%d", i, j)
			actualValue, err := rdb.Get(ctx, key).Result()
			if err == nil && actualValue == expectedValue {
				correctCount++
			}
		}
	}

	fmt.Printf("   ✅ 数据验证: %d/%d 正确\n", correctCount, totalOps)
}

// 连接信息测试
func testConnectionInfoV2(ctx context.Context, rdb *redis.Client) {
	fmt.Println("   测试连接信息...")

	// PING
	pong, err := rdb.Ping(ctx).Result()
	checkError("PING", err)
	fmt.Printf("   ✅ PING → %s\n", pong)

	// 连接池统计
	stats := rdb.PoolStats()
	fmt.Printf("   ✅ 连接池统计:\n")
	fmt.Printf("      总连接数: %d\n", stats.TotalConns)
	fmt.Printf("      空闲连接数: %d\n", stats.IdleConns)
	fmt.Printf("      过期连接数: %d\n", stats.StaleConns)
	fmt.Printf("      命中数: %d\n", stats.Hits)
	fmt.Printf("      未命中数: %d\n", stats.Misses)
	fmt.Printf("      超时数: %d\n", stats.Timeouts)

	// 随机测试一些INFO命令
	info, err := rdb.Info(ctx, "server").Result()
	if err == nil {
		lines := strings.Split(info, "\n")
		fmt.Printf("   ✅ Redis服务器信息 (前3行):\n")
		count := 0
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line != "" && !strings.HasPrefix(line, "#") {
				fmt.Printf("      %s\n", line)
				count++
				if count >= 3 {
					break
				}
			}
		}
	}
}

// 清理测试数据
func cleanupTestData(ctx context.Context, rdb *redis.Client) {
	// 获取所有测试key
	keys, err := rdb.Keys(ctx, "test:*").Result()
	if err != nil {
		fmt.Printf("   获取测试keys失败: %v\n", err)
		return
	}

	if len(keys) > 0 {
		deleted, err := rdb.Del(ctx, keys...).Result()
		if err != nil {
			fmt.Printf("   清理失败: %v\n", err)
		} else {
			fmt.Printf("   清理了 %d 个测试key\n", deleted)
		}
	} else {
		fmt.Printf("   没有需要清理的测试数据\n")
	}
}

// 错误检查辅助函数
func checkError(operation string, err error) {
	if err != nil {
		if err == redis.Nil {
			fmt.Printf("   ⚠️  %s → (nil)\n", operation)
		} else {
			fmt.Printf("   ❌ %s 失败: %v\n", operation, err)
		}
	}
}
