package until

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// MQ 全局配置（保持与你的 RabbitMQ 部署一致）
const (
	rabbitURL          = "amqp://admin:123456@localhost:5672/"
	AccessLogQueueName = "access-log-queue" // 访问日志队列

	ResultCachePrefix = "query-result:"     // 结果缓存前缀（轮询用）
	FuzzyCachePrefix  = "fuzzy:cache:"      // 模糊查询缓存前缀
	FuzzyLockPrefix   = "fuzzy:lock:"       // 模糊查询分布式锁前缀
	FuzzyQueueName    = "fuzzy-query-queue" // 模糊查询 MQ 队列
	FuzzyCacheExpire  = 10 * time.Minute    // 缓存过期时间（10 分钟）
)

// 全局 MQ 信道（单例，避免重复创建）
var mqChannel *amqp091.Channel
var mqConn *amqp091.Connection // 保存连接，便于程序退出时关闭

// InitMQ 初始化 MQ 连接和信道（程序启动时调用）
func InitMQ() error {

	conn, err := amqp091.DialConfig(
		rabbitURL,
		amqp091.Config{
			Heartbeat: 10 * time.Second, // 心跳间隔
			Locale:    "en_US",          // 本地化设置
		},
	)
	if err != nil {
		return fmt.Errorf("mq connect failed: %w", err)
	}
	mqConn = conn

	// 创建信道
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("create mq channel failed: %w", err)
	}
	mqChannel = ch

	// 声明队列（持久化、非自动删除、非排他）
	queues := []string{AccessLogQueueName, FuzzyQueueName}
	for _, queue := range queues {
		_, err := ch.QueueDeclare(
			queue,
			true,  // durable: 队列持久化
			false, // autoDelete: 不自动删除
			false, // exclusive: 非排他
			false, // noWait: 无等待
			nil,   // 额外参数
		)
		if err != nil {
			return fmt.Errorf("declare queue %s failed: %w", queue, err)
		}
	}

	log.Println("MQ 初始化成功（amqp091-go）")
	return nil
}

// PublishMQ 发送 MQ 消息（通用函数，支持上下文）
func PublishMQ(ctx context.Context, queueName string, body []byte) error {
	if mqChannel == nil {
		return fmt.Errorf("mq channel not initialized")
	}

	// 发送消息（带上下文，支持超时控制）
	return mqChannel.PublishWithContext(
		ctx,
		"",        // 默认交换机
		queueName, // 队列名（路由键）
		false,     // mandatory: 消息无法路由时是否返回
		false,     // immediate: 无消费者时是否立即返回（AMQP 0-9-1 已废弃，仅兼容）
		amqp091.Publishing{
			DeliveryMode: amqp091.Persistent, // 消息持久化
			ContentType:  "text/plain",       // 消息类型
			Body:         body,               // 消息体
			Timestamp:    time.Now(),         // 时间戳（可选）
		},
	)
}

// CloseMQ 关闭 MQ 连接和信道（程序退出时调用）
func CloseMQ() error {
	if mqChannel != nil {
		if err := mqChannel.Close(); err != nil {
			log.Printf("close mq channel failed: %v", err)
		}
	}
	if mqConn != nil {
		return mqConn.Close()
	}
	return nil
}
