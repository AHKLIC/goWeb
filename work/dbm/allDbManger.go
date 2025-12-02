package dbm

import (
	"context"
	"fmt"
	"github/AHKLIC/Web/work/config"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DbManger struct {
	MongoManger
	RedisManger *RedisManger
}

var (
	AllDbManger *DbManger
)

func NewDbManger() (*DbManger, error) {
	cfg := config.GetGlobalConfig()
	mongoUrl := cfg.MongoURL
	redisSentinelArr := cfg.RedisSentinelArr
	var mongoClient *mongo.Client

	mongoCli, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoUrl))
	if err != nil {
		return nil, fmt.Errorf("init mongo client failed: %w", err)
	}
	if err := mongoCli.Ping(context.Background(), nil); err != nil {
		return nil, fmt.Errorf("ping mongo failed: %w", err)
	}

	mongoClient = mongoCli

	sentinelOpts := &redis.FailoverOptions{
		MasterName:    "mymaster",       // 哨兵监控的主节点名称（必须与哨兵配置一致）
		SentinelAddrs: redisSentinelArr, // 哨兵节点地址列表
		Password:      "123456",         // Redis 节点密码（与集群配置一致）
		DB:            0,                // 默认数据库索引
		// 连接池配置（按需调整，优化性能）
		PoolSize:     100, // 最大连接数（默认：CPU 核心数 * 10）
		MinIdleConns: 10,  // 最小空闲连接数（避免频繁创建连接）
		// 超时配置（避免卡死）
		DialTimeout:  5 * time.Second, // 连接超时
		ReadTimeout:  3 * time.Second, // 读超时
		WriteTimeout: 3 * time.Second, // 写超时
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			// addr 是哨兵返回的主节点地址（如 172.28.0.10:6379）
			// 根据主节点容器内 IP，映射到对应的主机端口
			switch addr {
			case "172.28.0.10:6379": // 原主节点 → 主机端口 6379
				addr = "localhost:6379"
			case "172.28.0.11:6379": // 从节点1 → 主机端口 6380
				addr = "localhost:6380"
			case "172.28.0.12:6379": // 从节点2 → 主机端口 6381
				addr = "localhost:6381"
			}
			// 用替换后的地址拨号连接
			return net.DialTimeout(network, addr, 5*time.Second)
		},
	}

	redisManage, err := NewRedisManager(sentinelOpts, 6)
	if err != nil {
		return nil, fmt.Errorf("init redis manager failed: %w", err)
	}

	return &DbManger{
		MongoManger: MongoManger{mongoClient: mongoClient,
			mongodbDatasName: cfg.MongoDBNameData,
			mongodbUsersName: cfg.MongoDBNameUsers,
		},
		RedisManger: redisManage,
	}, nil

}
