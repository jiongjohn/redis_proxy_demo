package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// 核心命令测试 - 专门测试 SET, GET, SADD, SETNX, HSET, HGET 等核心命令
func runCoreCommandsTest() {
	fmt.Println("🎯 Redis 核心命令测试")
	fmt.Println(strings.Repeat("=", 50))

	// 创建Redis客户端
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6380", // 连接到代理端口
		Password: "",
		DB:       0,

		// 连接池配置
		PoolSize:        10,
		MinIdleConns:    2,
		MaxIdleConns:    5,
		ConnMaxIdleTime: 30 * time.Second,
		ConnMaxLifetime: 5 * time.Minute,

		// 超时配置
		DialTimeout:  5 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	})

	ctx := context.Background()

	// 清理旧数据
	fmt.Println("\n🧹 清理旧测试数据...")
	cleanupCoreTestData(ctx, rdb)

	// 1. 字符串核心命令测试
	fmt.Println("\n📝 1. 字符串核心命令测试")
	testCoreStringCommands(ctx, rdb)

	// 2. 哈希核心命令测试
	fmt.Println("\n🗂️  2. 哈希核心命令测试")
	testCoreHashCommands(ctx, rdb)

	// 3. 集合核心命令测试
	fmt.Println("\n🎯 3. 集合核心命令测试")
	testCoreSetCommands(ctx, rdb)

	// 4. 列表核心命令测试
	fmt.Println("\n📋 4. 列表核心命令测试")
	testCoreListCommands(ctx, rdb)

	// 5. 原子操作测试
	fmt.Println("\n⚛️  5. 原子操作测试")
	testCoreAtomicCommands(ctx, rdb)

	// 6. 过期命令测试
	fmt.Println("\n⏰ 6. 过期命令测试")
	testCoreExpirationCommands(ctx, rdb)

	// 7. 性能基准测试
	fmt.Println("\n🏆 7. 核心命令性能测试")
	testCoreCommandsPerformance(ctx, rdb)

	// 最终清理
	fmt.Println("\n🧹 清理测试数据...")
	cleanupCoreTestData(ctx, rdb)

	// 关闭连接
	if err := rdb.Close(); err != nil {
		log.Printf("关闭Redis客户端失败: %v", err)
	}

	fmt.Println("\n✅ 核心命令测试完成！")
}

// 字符串核心命令测试
func testCoreStringCommands(ctx context.Context, rdb *redis.Client) {
	fmt.Println("   测试: SET, GET, SETNX, GETSET, INCR, DECR, APPEND")

	// SET 命令
	err := rdb.Set(ctx, "core:str:name", "Redis代理测试", 0).Err()
	if err != nil {
		fmt.Printf("   ❌ SET 失败: %v\n", err)
	} else {
		fmt.Println("   ✅ SET core:str:name 成功")
	}

	// GET 命令
	val, err := rdb.Get(ctx, "core:str:name").Result()
	if err != nil {
		fmt.Printf("   ❌ GET 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ GET core:str:name → %s\n", val)
	}

	// SETNX 命令 (SET if Not eXists)
	success, err := rdb.SetNX(ctx, "core:str:unique", "唯一值", 0).Result()
	if err != nil {
		fmt.Printf("   ❌ SETNX 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ SETNX core:str:unique → %t (首次设置)\n", success)
	}

	// 再次SETNX应该返回false
	success, err = rdb.SetNX(ctx, "core:str:unique", "另一个值", 0).Result()
	if err != nil {
		fmt.Printf("   ❌ SETNX 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ SETNX core:str:unique → %t (重复设置)\n", success)
	}

	// GETSET 命令
	oldVal, err := rdb.GetSet(ctx, "core:str:name", "新的Redis代理测试").Result()
	if err != nil {
		fmt.Printf("   ❌ GETSET 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ GETSET core:str:name → 旧值: %s\n", oldVal)
	}

	// 数值操作
	rdb.Set(ctx, "core:str:counter", "100", 0)

	// INCR 命令
	newVal, err := rdb.Incr(ctx, "core:str:counter").Result()
	if err != nil {
		fmt.Printf("   ❌ INCR 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ INCR core:str:counter → %d\n", newVal)
	}

	// DECR 命令
	newVal, err = rdb.Decr(ctx, "core:str:counter").Result()
	if err != nil {
		fmt.Printf("   ❌ DECR 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ DECR core:str:counter → %d\n", newVal)
	}

	// INCRBY 命令
	newVal, err = rdb.IncrBy(ctx, "core:str:counter", 10).Result()
	if err != nil {
		fmt.Printf("   ❌ INCRBY 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ INCRBY core:str:counter 10 → %d\n", newVal)
	}

	// APPEND 命令
	length, err := rdb.Append(ctx, "core:str:name", " - 追加内容").Result()
	if err != nil {
		fmt.Printf("   ❌ APPEND 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ APPEND core:str:name → 新长度: %d\n", length)
	}

	// 验证APPEND结果
	finalVal, _ := rdb.Get(ctx, "core:str:name").Result()
	fmt.Printf("   ✅ APPEND后的值: %s\n", finalVal)
}

// 哈希核心命令测试
func testCoreHashCommands(ctx context.Context, rdb *redis.Client) {
	fmt.Println("   测试: HSET, HGET, HMSET, HMGET, HGETALL, HDEL, HEXISTS")

	// HSET 命令 (单个字段)
	err := rdb.HSet(ctx, "core:hash:user", "name", "张三").Err()
	if err != nil {
		fmt.Printf("   ❌ HSET 失败: %v\n", err)
	} else {
		fmt.Println("   ✅ HSET core:hash:user name 张三")
	}

	// HSET 命令 (多个字段)
	err = rdb.HSet(ctx, "core:hash:user",
		"age", "25",
		"city", "北京",
		"job", "工程师",
	).Err()
	if err != nil {
		fmt.Printf("   ❌ HSET 多字段失败: %v\n", err)
	} else {
		fmt.Println("   ✅ HSET core:hash:user 多字段设置成功")
	}

	// HGET 命令
	name, err := rdb.HGet(ctx, "core:hash:user", "name").Result()
	if err != nil {
		fmt.Printf("   ❌ HGET 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ HGET core:hash:user name → %s\n", name)
	}

	// HMGET 命令 (批量获取)
	values, err := rdb.HMGet(ctx, "core:hash:user", "name", "age", "city").Result()
	if err != nil {
		fmt.Printf("   ❌ HMGET 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ HMGET core:hash:user name,age,city → %v\n", values)
	}

	// HGETALL 命令
	allData, err := rdb.HGetAll(ctx, "core:hash:user").Result()
	if err != nil {
		fmt.Printf("   ❌ HGETALL 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ HGETALL core:hash:user → %v\n", allData)
	}

	// HEXISTS 命令
	exists, err := rdb.HExists(ctx, "core:hash:user", "name").Result()
	if err != nil {
		fmt.Printf("   ❌ HEXISTS 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ HEXISTS core:hash:user name → %t\n", exists)
	}

	// HLEN 命令
	length, err := rdb.HLen(ctx, "core:hash:user").Result()
	if err != nil {
		fmt.Printf("   ❌ HLEN 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ HLEN core:hash:user → %d\n", length)
	}

	// HDEL 命令
	deleted, err := rdb.HDel(ctx, "core:hash:user", "job").Result()
	if err != nil {
		fmt.Printf("   ❌ HDEL 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ HDEL core:hash:user job → %d\n", deleted)
	}

	// HINCRBY 命令
	rdb.HSet(ctx, "core:hash:stats", "score", "100")
	newScore, err := rdb.HIncrBy(ctx, "core:hash:stats", "score", 25).Result()
	if err != nil {
		fmt.Printf("   ❌ HINCRBY 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ HINCRBY core:hash:stats score 25 → %d\n", newScore)
	}
}

// 集合核心命令测试
func testCoreSetCommands(ctx context.Context, rdb *redis.Client) {
	fmt.Println("   测试: SADD, SMEMBERS, SCARD, SISMEMBER, SPOP, SREM")

	// SADD 命令
	added, err := rdb.SAdd(ctx, "core:set:tags", "redis", "数据库", "缓存", "nosql", "内存").Result()
	if err != nil {
		fmt.Printf("   ❌ SADD 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ SADD core:set:tags → 添加了 %d 个成员\n", added)
	}

	// SCARD 命令 (获取集合大小)
	count, err := rdb.SCard(ctx, "core:set:tags").Result()
	if err != nil {
		fmt.Printf("   ❌ SCARD 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ SCARD core:set:tags → %d\n", count)
	}

	// SMEMBERS 命令
	members, err := rdb.SMembers(ctx, "core:set:tags").Result()
	if err != nil {
		fmt.Printf("   ❌ SMEMBERS 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ SMEMBERS core:set:tags → %v\n", members)
	}

	// SISMEMBER 命令
	isMember, err := rdb.SIsMember(ctx, "core:set:tags", "redis").Result()
	if err != nil {
		fmt.Printf("   ❌ SISMEMBER 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ SISMEMBER core:set:tags redis → %t\n", isMember)
	}

	isMember, err = rdb.SIsMember(ctx, "core:set:tags", "mysql").Result()
	if err != nil {
		fmt.Printf("   ❌ SISMEMBER 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ SISMEMBER core:set:tags mysql → %t\n", isMember)
	}

	// SPOP 命令 (随机弹出一个成员)
	poppedMember, err := rdb.SPop(ctx, "core:set:tags").Result()
	if err != nil {
		fmt.Printf("   ❌ SPOP 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ SPOP core:set:tags → %s\n", poppedMember)
	}

	// SRANDMEMBER 命令 (随机获取成员，不删除)
	randomMember, err := rdb.SRandMember(ctx, "core:set:tags").Result()
	if err != nil {
		fmt.Printf("   ❌ SRANDMEMBER 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ SRANDMEMBER core:set:tags → %s\n", randomMember)
	}

	// SREM 命令
	removed, err := rdb.SRem(ctx, "core:set:tags", "缓存").Result()
	if err != nil {
		fmt.Printf("   ❌ SREM 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ SREM core:set:tags 缓存 → %d\n", removed)
	}

	// 查看最终的集合成员
	finalMembers, _ := rdb.SMembers(ctx, "core:set:tags").Result()
	fmt.Printf("   ✅ 最终集合成员: %v\n", finalMembers)
}

// 列表核心命令测试
func testCoreListCommands(ctx context.Context, rdb *redis.Client) {
	fmt.Println("   测试: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN")

	// LPUSH 命令 (左侧插入)
	length, err := rdb.LPush(ctx, "core:list:queue", "任务1", "任务2", "任务3").Result()
	if err != nil {
		fmt.Printf("   ❌ LPUSH 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ LPUSH core:list:queue → 列表长度: %d\n", length)
	}

	// RPUSH 命令 (右侧插入)
	length, err = rdb.RPush(ctx, "core:list:queue", "任务4", "任务5").Result()
	if err != nil {
		fmt.Printf("   ❌ RPUSH 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ RPUSH core:list:queue → 列表长度: %d\n", length)
	}

	// LLEN 命令
	length, err = rdb.LLen(ctx, "core:list:queue").Result()
	if err != nil {
		fmt.Printf("   ❌ LLEN 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ LLEN core:list:queue → %d\n", length)
	}

	// LRANGE 命令 (查看所有元素)
	items, err := rdb.LRange(ctx, "core:list:queue", 0, -1).Result()
	if err != nil {
		fmt.Printf("   ❌ LRANGE 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ LRANGE core:list:queue 0 -1 → %v\n", items)
	}

	// LPOP 命令 (左侧弹出)
	leftItem, err := rdb.LPop(ctx, "core:list:queue").Result()
	if err != nil {
		fmt.Printf("   ❌ LPOP 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ LPOP core:list:queue → %s\n", leftItem)
	}

	// RPOP 命令 (右侧弹出)
	rightItem, err := rdb.RPop(ctx, "core:list:queue").Result()
	if err != nil {
		fmt.Printf("   ❌ RPOP 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ RPOP core:list:queue → %s\n", rightItem)
	}

	// LINDEX 命令 (获取指定位置的元素)
	item, err := rdb.LIndex(ctx, "core:list:queue", 0).Result()
	if err != nil {
		fmt.Printf("   ❌ LINDEX 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ LINDEX core:list:queue 0 → %s\n", item)
	}

	// 查看最终的列表
	finalItems, _ := rdb.LRange(ctx, "core:list:queue", 0, -1).Result()
	fmt.Printf("   ✅ 最终列表: %v\n", finalItems)
}

// 原子操作测试
func testCoreAtomicCommands(ctx context.Context, rdb *redis.Client) {
	fmt.Println("   测试原子操作: INCR, DECR, INCRBY, DECRBY")

	// 初始化计数器
	rdb.Set(ctx, "core:atomic:visits", "1000", 0)

	// INCR 原子递增
	newVal, err := rdb.Incr(ctx, "core:atomic:visits").Result()
	if err != nil {
		fmt.Printf("   ❌ INCR 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ INCR core:atomic:visits → %d\n", newVal)
	}

	// INCRBY 原子增加指定值
	newVal, err = rdb.IncrBy(ctx, "core:atomic:visits", 50).Result()
	if err != nil {
		fmt.Printf("   ❌ INCRBY 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ INCRBY core:atomic:visits 50 → %d\n", newVal)
	}

	// DECR 原子递减
	newVal, err = rdb.Decr(ctx, "core:atomic:visits").Result()
	if err != nil {
		fmt.Printf("   ❌ DECR 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ DECR core:atomic:visits → %d\n", newVal)
	}

	// DECRBY 原子减少指定值
	newVal, err = rdb.DecrBy(ctx, "core:atomic:visits", 25).Result()
	if err != nil {
		fmt.Printf("   ❌ DECRBY 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ DECRBY core:atomic:visits 25 → %d\n", newVal)
	}

	// 测试SETNX的原子性 (分布式锁)
	lockKey := "core:atomic:lock"
	success1, _ := rdb.SetNX(ctx, lockKey, "进程1", 5*time.Second).Result()
	success2, _ := rdb.SetNX(ctx, lockKey, "进程2", 5*time.Second).Result()

	fmt.Printf("   ✅ 分布式锁测试: 进程1获取锁=%t, 进程2获取锁=%t\n", success1, success2)

	// 清理锁
	rdb.Del(ctx, lockKey)
}

// 过期命令测试
func testCoreExpirationCommands(ctx context.Context, rdb *redis.Client) {
	fmt.Println("   测试: EXPIRE, TTL, SETEX, PERSIST")

	// 设置一个key
	rdb.Set(ctx, "core:expire:session", "用户会话数据", 0)

	// EXPIRE 命令
	err := rdb.Expire(ctx, "core:expire:session", 30*time.Second).Err()
	if err != nil {
		fmt.Printf("   ❌ EXPIRE 失败: %v\n", err)
	} else {
		fmt.Println("   ✅ EXPIRE core:expire:session 30秒")
	}

	// TTL 命令
	ttl, err := rdb.TTL(ctx, "core:expire:session").Result()
	if err != nil {
		fmt.Printf("   ❌ TTL 失败: %v\n", err)
	} else {
		fmt.Printf("   ✅ TTL core:expire:session → %v\n", ttl)
	}

	// SETEX 命令 (设置key并指定过期时间)
	err = rdb.SetEx(ctx, "core:expire:temp", "临时数据", 10*time.Second).Err()
	if err != nil {
		fmt.Printf("   ❌ SETEX 失败: %v\n", err)
	} else {
		fmt.Println("   ✅ SETEX core:expire:temp 10秒")
	}

	// 检查SETEX的TTL
	ttl, _ = rdb.TTL(ctx, "core:expire:temp").Result()
	fmt.Printf("   ✅ SETEX设置的TTL: %v\n", ttl)

	// PERSIST 命令 (移除过期时间)
	err = rdb.Persist(ctx, "core:expire:session").Err()
	if err != nil {
		fmt.Printf("   ❌ PERSIST 失败: %v\n", err)
	} else {
		fmt.Println("   ✅ PERSIST core:expire:session")
	}

	// 再次检查TTL
	ttl, _ = rdb.TTL(ctx, "core:expire:session").Result()
	fmt.Printf("   ✅ PERSIST后的TTL: %v\n", ttl)
}

// 核心命令性能测试
func testCoreCommandsPerformance(ctx context.Context, rdb *redis.Client) {
	fmt.Println("   性能测试各核心命令...")

	const numOps = 1000

	// SET 性能测试
	start := time.Now()
	for i := 0; i < numOps; i++ {
		rdb.Set(ctx, fmt.Sprintf("perf:set:%d", i), fmt.Sprintf("value%d", i), 0)
	}
	setDuration := time.Since(start)
	fmt.Printf("   ✅ SET性能: %d次操作, 耗时 %v, 速率 %.2f ops/sec\n",
		numOps, setDuration, float64(numOps)/setDuration.Seconds())

	// GET 性能测试
	start = time.Now()
	for i := 0; i < numOps; i++ {
		rdb.Get(ctx, fmt.Sprintf("perf:set:%d", i))
	}
	getDuration := time.Since(start)
	fmt.Printf("   ✅ GET性能: %d次操作, 耗时 %v, 速率 %.2f ops/sec\n",
		numOps, getDuration, float64(numOps)/getDuration.Seconds())

	// HSET 性能测试
	start = time.Now()
	for i := 0; i < numOps; i++ {
		rdb.HSet(ctx, "perf:hash", fmt.Sprintf("field%d", i), fmt.Sprintf("value%d", i))
	}
	hsetDuration := time.Since(start)
	fmt.Printf("   ✅ HSET性能: %d次操作, 耗时 %v, 速率 %.2f ops/sec\n",
		numOps, hsetDuration, float64(numOps)/hsetDuration.Seconds())

	// SADD 性能测试
	start = time.Now()
	for i := 0; i < numOps; i++ {
		rdb.SAdd(ctx, "perf:set", fmt.Sprintf("member%d", i))
	}
	saddDuration := time.Since(start)
	fmt.Printf("   ✅ SADD性能: %d次操作, 耗时 %v, 速率 %.2f ops/sec\n",
		numOps, saddDuration, float64(numOps)/saddDuration.Seconds())

	// INCR 性能测试
	rdb.Set(ctx, "perf:counter", "0", 0)
	start = time.Now()
	for i := 0; i < numOps; i++ {
		rdb.Incr(ctx, "perf:counter")
	}
	incrDuration := time.Since(start)
	fmt.Printf("   ✅ INCR性能: %d次操作, 耗时 %v, 速率 %.2f ops/sec\n",
		numOps, incrDuration, float64(numOps)/incrDuration.Seconds())

	// 清理性能测试数据
	keys, _ := rdb.Keys(ctx, "perf:*").Result()
	if len(keys) > 0 {
		rdb.Del(ctx, keys...)
	}
}

// 清理核心测试数据
func cleanupCoreTestData(ctx context.Context, rdb *redis.Client) {
	keys, err := rdb.Keys(ctx, "core:*").Result()
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

func main() {
	runCoreCommandsTest()
}
