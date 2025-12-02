package until

import (
	"context"
	"encoding/json"
	"github/AHKLIC/Web/work/dbm"
	"log/slog"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// StartMQConsumers 启动所有 MQ 消费者（程序启动时调用）
func StartMQConsumers(ctx context.Context) {
	// // 1. 启动无效键清理消费者
	// go startInvalidKeyConsumer(context.Background(), redisClient)
	// 2. 启动访问日志消费者
	go startAccessLogConsumer(ctx)
	go startFuzzyQueryConsumer(ctx)
	// 3. 启动数据更新消费者
	// go startDataUpdateConsumer(context.Background(), redisClient)

	slog.Info("所有 MQ 消费者启动成功（amqp091-go）")
}

// 2. 访问日志消费者：异步记录日志
func startAccessLogConsumer(ctx context.Context) {
	msgs, err := mqChannel.Consume(
		AccessLogQueueName,
		"access-log-consumer",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		slog.Error("启动访问日志消费者失败", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			slog.Info("访问日志消费者退出")
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			var logData LogLayout
			if err := json.Unmarshal(msg.Body, &logData); err != nil {
				slog.Error("failed to unmarshal access log", "error", err)
			}
			if logData.Status == 200 {
				// 结构化输出，避免转义
				slog.Info("access log",
					"method", logData.Method,
					"path", logData.Path,
					"query", logData.Query,
					"ip", logData.IP,
					"user_agent", logData.UserAgent,
					"error", logData.Error,
					"cost", logData.Cost,
					"status", logData.Status,
				)
			} else {
				slog.Error("access log",
					"method", logData.Method,
					"path", logData.Path,
					"query", logData.Query,
					"ip", logData.IP,
					"user_agent", logData.UserAgent,
					"error", logData.Error,
					"cost", logData.Cost,
					"status", logData.Status,
				)
			}
			_ = msg.Ack(false)
		}
	}
}

// startFuzzyQueryConsumer 模糊查询消费者（异步查 DB + 写缓存）
func startFuzzyQueryConsumer(ctx context.Context) {
	// 注册消费者
	redisClient := dbm.AllDbManger.RedisManger.GetMasterClient()
	msgs, err := mqChannel.Consume(
		FuzzyQueueName,
		"fuzzy-query-consumer",
		false, // 手动确认
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		slog.Error("启动模糊查询消费者失败", "error", err)
	}

	// 并发控制（削峰：限制 8 个并发查询 DB）
	concurrency := 8
	sem := make(chan struct{}, concurrency)

	slog.Info("模糊查询消费者启动成功", "并发处理数", concurrency)

	for {
		select {
		case <-ctx.Done():
			slog.Info("模糊查询消费者退出")
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}

			sem <- struct{}{} // 占用信号量
			go func(msg amqp091.Delivery) {
				defer func() {
					<-sem // 释放信号量
					if r := recover(); r != nil {
						slog.Error("模糊查询消费者", "panic:", r)
						_ = msg.Nack(false, true)
					}
				}()

				// 解析消息
				var msgData map[string]string
				if err := json.Unmarshal(msg.Body, &msgData); err != nil {
					slog.Error("解析模糊查询消息失败:", "error", err)
					_ = msg.Ack(false)
					return
				}
				keyword := msgData["keyword"]
				cacheKey := GetFuzzyCacheKey(keyword)
				slog.Info("开始模糊查询", "keyword:", keyword)
				resultList, err := dbm.AllDbManger.MongoManger.GetMongoDataFuzzyByKeyword(keyword)
				if err != nil {
					slog.Error("模糊查询数据库失败", "keyword", keyword, "error", err)
				}

				// 2. 结果序列化
				resultJSON, _ := json.Marshal(resultList)
				slog.Info("模糊查询成功", "keyword:", keyword)

				// 3. 写入 Redis 缓存（状态改为 ready）
				redisClient.HSet(
					ctx, cacheKey,
					"status", "ready",
					"data", string(resultJSON),
					"update_time", time.Now().Format("2006-01-02 15:04:05"),
				)
				redisClient.Expire(ctx, cacheKey, FuzzyCacheExpire) // 10 分钟过期

				// 4. 确认消息
				_ = msg.Ack(false)
			}(msg)
		}
	}
}
