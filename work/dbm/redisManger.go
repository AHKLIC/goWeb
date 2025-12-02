package dbm

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisManger 管理 Redis 哨兵集群连接（v9 版本）
type RedisManger struct {
	masterClient *redis.Client          // 主节点客户端（仅用于写操作）
	slaveClients []*redis.Client        // 从节点客户端列表（用于读操作）
	maxBatches   int                    // 每个source最大数据批次数
	sentinelOpts *redis.FailoverOptions // 哨兵配置（用于刷新主从节点）
	mu           sync.RWMutex           // 保护从节点列表的并发安全
	rand         *rand.Rand             // 用于随机选择从节点
}

func (r *RedisManger) GetMasterClient() *redis.Client {

	return r.masterClient
}

func (r *RedisManger) GetSlaveClient() (*redis.Client, error) {
	readclient, err := r.selectReadClient()
	if err != nil {
		return nil, fmt.Errorf("get readClinet fail %w", err)
	}
	return readclient, nil
}

// NewRedisManager 初始化 RedisManager（哨兵集群+读写分离）
func NewRedisManager(sentinelOpts *redis.FailoverOptions, maxBatches int) (*RedisManger, error) {
	// 1. 创建主节点客户端
	masterClient := redis.NewFailoverClient(sentinelOpts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := masterClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("connect to master failed: %w", err)
	}

	// 2. 获取从节点地址
	slaveAddrs, err := getSlaveAddrs(sentinelOpts)
	if err != nil {
		return nil, fmt.Errorf("get slave addresses failed: %w", err)
	}

	// 3. 为每个从节点创建客户端
	var slaveClients []*redis.Client
	for _, addr := range slaveAddrs {

		switch addr {
		case "172.28.0.10:6379": // 原主节点 → 主机端口 6379
			addr = "localhost:6379"
		case "172.28.0.11:6379": // 从节点1 → 主机端口 6380
			addr = "localhost:6380"
		case "172.28.0.12:6379": // 从节点2 → 主机端口 6381
			addr = "localhost:6381"
		}
		client := redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: sentinelOpts.Password,
			DB:       sentinelOpts.DB,
		})
		// 验证连接
		if err := client.Ping(context.Background()).Err(); err != nil {
			client.Close()
			continue // 跳过不可用从节点
		}
		slaveClients = append(slaveClients, client)
	}

	if len(slaveClients) == 0 {
		slog.Warn("no available slave nodes")
	}
	slog.Info("RedisManager: 1 master", "slave_count", len(slaveClients))
	rm := &RedisManger{
		masterClient: masterClient,
		slaveClients: slaveClients,
		maxBatches:   maxBatches,
		sentinelOpts: sentinelOpts,
		rand:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	// 4. 启动后台协程：定期刷新主从节点列表（每30秒，可调整）
	go rm.refreshSlaveClientsLoop(30 * time.Second)

	return rm, nil

}

func (r *RedisManger) GetLatestDataBySource(ctx context.Context, source string) (interface{}, error) {
	// 1. 获取该source的ZSet键
	zsetKey := fmt.Sprintf("hot:zset:%s", source)
	readClient, err := r.selectReadClient()
	if err != nil {
		return nil, fmt.Errorf("select readClient failed: %w", err)
	}
	// 2. 从ZSet中获取score最大的1个成员（最新数据键）
	// ZREVRANGE：按score降序排列，取第0个（最新）
	latestMembers, err := readClient.ZRevRange(ctx, zsetKey, 0, 0).Result()
	if err != nil {
		return nil, fmt.Errorf("get latest data key failed: %w", err)
	}

	// 3. 无数据时返回空
	if len(latestMembers) == 0 {
		return nil, fmt.Errorf("no data found for source: %s", source)
	}

	// 4. 根据数据键查询具体数据
	latestDataKey := latestMembers[0]
	jsonStr, err := readClient.Get(ctx, latestDataKey).Result()
	if err != nil {
		// 若数据键已过期（但ZSet未清理），删除ZSet中的无效成员
		if err == redis.Nil {
			_ = r.masterClient.ZRem(ctx, zsetKey, latestDataKey)
			return nil, fmt.Errorf("latest data key expired: %s", latestDataKey)
		}
		return nil, fmt.Errorf("get data from redis failed: %w", err)
	}

	// 5. 反序列化为interface{}（或根据实际业务结构反序列化为具体结构体）
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return nil, fmt.Errorf("unmarshal json failed: %w", err)
	}

	return data, nil
}

func (r *RedisManger) GetDataByKey(ctx context.Context, cacheKey string) (interface{}, string, error) {

	readClient, err := r.selectReadClient()
	if err != nil {
		return nil, "", fmt.Errorf("select readClient failed: %w", err)
	}
	var data interface{}
	cacheData, err := readClient.HGetAll(ctx, cacheKey).Result()

	if err == nil && len(cacheData) > 0 && cacheData["status"] == "ready" {
		// 缓存命中，反序列化数据返回
		json.Unmarshal([]byte(cacheData["data"]), &data)
		return data, "", nil
	}
	return nil, cacheData["status"], err
}

// GetAllSourcesLatestData 查询所有source的最新数据（可选）
// func (r *RedisManger) GetAllSourcesLatestData(ctx context.Context) (map[string]interface{}, error) {
// 	// 1. 模糊匹配所有source的ZSet键（hot:zset:*）
// 	zsetKeys, err := r.masterClient.Keys(ctx, "hot:zset:*").Result()
// 	if err != nil {
// 		return nil, fmt.Errorf("list all zset keys failed: %w", err)
// 	}

// 	latestDataMap := make(map[string]interface{})
// 	for _, zsetKey := range zsetKeys {
// 		// 提取source名称（从hot:zset:source中截取）
// 		source := zsetKey[len("hot:zset:"):]
// 		// 查询该source的最新数据
// 		data, err := r.GetLatestDataBySource(ctx, source)
// 		if err != nil {
// 			fmt.Printf("warn: get latest data for source %s failed: %v\n", source, err)
// 			continue
// 		}
// 		latestDataMap[source] = data
// 	}

// 	return latestDataMap, nil
// }

// refreshSlaveClientsLoop 定期刷新从节点列表（应对主从切换）
func (r *RedisManger) refreshSlaveClientsLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		r.mu.Lock()
		// 重新获取从节点列表
		slaveAddrs, err := getSlaveAddrs(r.sentinelOpts)
		if err != nil {
			slog.Error("get slave addresses failed", "error", err)
		}

		// 3. 为每个从节点创建客户端
		var slaveClients []*redis.Client
		for _, addr := range slaveAddrs {
			switch addr {
			case "172.28.0.10:6379": // 原主节点 → 主机端口 6379
				addr = "localhost:6379"
			case "172.28.0.11:6379": // 从节点1 → 主机端口 6380
				addr = "localhost:6380"
			case "172.28.0.12:6379": // 从节点2 → 主机端口 6381
				addr = "localhost:6381"
			}
			client := redis.NewClient(&redis.Options{
				Addr:     addr,
				Password: r.sentinelOpts.Password,
				DB:       r.sentinelOpts.DB,
			})
			// 验证连接
			if err := client.Ping(context.Background()).Err(); err != nil {
				client.Close()
				continue // 跳过不可用从节点
			}
			slaveClients = append(slaveClients, client)
		}
		if err != nil {
			slog.Error("refresh slave clients failed", "error", err)
			r.mu.Unlock()
			continue
		}

		// 关闭旧的从节点客户端（避免资源泄漏）
		for _, oldClient := range r.slaveClients {
			_ = oldClient.Close()
		}

		// 更新从节点客户端列表
		r.slaveClients = slaveClients
		r.mu.Unlock()
	}
}

// getSlaveAddrs 通过哨兵获取从节点地址列表
func getSlaveAddrs(sentinelOpts *redis.FailoverOptions) ([]string, error) {
	if len(sentinelOpts.SentinelAddrs) == 0 {
		return nil, fmt.Errorf("sentinel address list is empty")
	}

	// 连接第一个哨兵
	sentinel := redis.NewClient(&redis.Options{
		Addr: sentinelOpts.SentinelAddrs[0],
	})
	defer sentinel.Close()

	// 执行 SENTINEL SLAVES <master-name>
	cmd := sentinel.Do(context.Background(), "SENTINEL", "SLAVES", sentinelOpts.MasterName)
	if cmd.Err() != nil {
		return nil, fmt.Errorf("SENTINEL SLAVES failed: %w", cmd.Err())
	}

	// 解析返回值
	slaves, ok := cmd.Val().([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid SENTINEL SLAVES response")
	}

	var addrs []string
	// 第二步：遍历每个从节点的 map 数据
	for _, slaveRaw := range slaves {
		// 将 slaveRaw 断言为 map[interface{}]interface{}（核心修正）
		slaveMap, ok := slaveRaw.(map[interface{}]interface{})
		if !ok {
			slog.Warn("warning: invalid slave data format (not map), skip")
			continue
		}

		// 第三步：从 map 中提取 "ip" 和 "port"（key 是 string 类型）
		ip := safeString(slaveMap["ip"])     // 按 key "ip" 取 IP 地址
		port := safeString(slaveMap["port"]) // 按 key "port" 取端口号

		// 验证 IP 和端口非空
		if ip != "" && port != "" {
			addr := net.JoinHostPort(ip, port)
			addrs = append(addrs, addr)
		} else {
			slog.Warn("warning: slave node missing ip/port, skip", "ip", ip, "port", port)
		}
	}

	if len(addrs) == 0 {
		return nil, fmt.Errorf("no valid slave nodes found")
	}

	return addrs, nil
}

// safeString 安全地将 interface{} 转换为 string（处理 nil、非 string 类型）
func safeString(v interface{}) string {
	if v == nil {
		return ""
	}
	// 尝试直接转换为 string
	if s, ok := v.(string); ok {
		return s
	}
	// 若值是数字类型（如 float64，部分 Redis 版本可能返回数字端口），转为 string
	if num, ok := v.(float64); ok {
		return fmt.Sprintf("%.0f", num) // 端口是整数，去掉小数部分
	}
	// 其他类型直接返回空字符串
	return ""
}

// selectReadClient 选择读操作的客户端（随机选择从节点，无从节点则用主节点）
func (r *RedisManger) selectReadClient() (*redis.Client, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// 若有从节点，随机选择一个
	if len(r.slaveClients) > 0 {
		idx := r.rand.Intn(len(r.slaveClients))
		return r.slaveClients[idx], nil
	}

	// 无从节点时，降级到主节点
	slog.Warn("no slave clients available, use master client for read")
	return r.masterClient, nil
}
