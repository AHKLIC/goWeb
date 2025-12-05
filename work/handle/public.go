package handle

import (
	"context"
	"fmt"

	"net/http"

	"encoding/json"
	"github/AHKLIC/Web/work/dbm"
	"github/AHKLIC/Web/work/until"
	"time"

	"github.com/gin-gonic/gin"
)

// 登录接口（公开）
func LoginHandler(c *gin.Context) {
	// 绑定请求参数（用户名/密码）
	type LoginRequest struct {
		Username string `json:"username" binding:"required"`
		Password string `json:"password" binding:"required"`
	}
	var req LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(&until.BusinessError{Code: 400, Message: "参数错误：" + err.Error()})
		return
	}

	// 模拟数据库验证（生产环境替换为真实数据库查询）
	if req.Username != "admin" || req.Password != "123456" {
		c.Error(&until.BusinessError{Code: 403, Message: "用户名或密码错误"})
		return
	}

	// 生成 JWT Token
	token, err := until.GenerateJWT(1001, req.Username)
	if err != nil {
		c.Error(&until.BusinessError{Code: 500, Message: "Token 生成失败"})
		return
	}

	// 返回成功响应
	c.JSON(http.StatusOK, until.Response{
		Code:    0,
		Message: "登录成功",
		Data:    gin.H{"token": token, "expire_hour": until.JWTExpireHour},
	})
}

// 健康检查接口（公开）
func HealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, until.Response{
		Code:    0,
		Message: "服务正常",
		Data:    gin.H{"time": time.Now().Format("2006-01-02 15:04:05")},
	})
}

// ?source=XXXX
func GetLatestCrawleData(c *gin.Context) {
	source := c.Query("source")
	if source == "" {
		c.Error(&until.BusinessError{Code: 400, Message: "参数错误：source不能为空"})
		return
	}
	data, err := dbm.AllDbManger.RedisManger.GetLatestDataBySource(c.Request.Context(), source)
	if err != nil {
		c.Error(&until.BusinessError{Code: 500, Message: "获取数据失败：" + err.Error()})
		return
	}

	c.JSON(http.StatusOK, until.Response{
		Code:    0,
		Message: "获取成功",
		Data:    data,
	})

}

// / api/public/query/fuzzy/search?keyword=XXX
func SubmitFuzzyQuery(c *gin.Context) {

	keyword := c.Query("keyword")
	var priority uint8
	userType := c.GetString("user_type")
	if userType == until.UserTypeVIP {
		priority = 10
	}
	if keyword == "" {
		c.Error(&until.BusinessError{Code: 400, Message: "参数错误：keyword不能为空"})
		return
	}

	// 1. 生成缓存键和锁键
	cacheKey := until.GetFuzzyCacheKey(keyword)
	lockKey := until.GetFuzzyLockKey(keyword)
	ctx := c.Request.Context()

	// 2. 查 Redis 缓存：如果已就绪，直接返回
	data, status, err := dbm.AllDbManger.RedisManger.GetDataByKey(ctx, cacheKey)
	if err != nil {
		c.Error(&until.BusinessError{Code: 500, Message: "获取数据失败：" + err.Error()})
		return
	}

	if data != nil {

		c.JSON(http.StatusOK, until.Response{
			Code:    0,
			Message: "获取成功",
			Data:    data,
		})
		return
	}

	// 3. 生成轮询用的请求 ID
	reqID := until.GenerateReqID()
	reqStatusKey := fmt.Sprintf("%s%s", until.ResultCachePrefix, reqID)

	// 4. 分布式锁：防止同一 keyword 被多个请求重复触发 DB 查询
	lockVal := until.GenerateReqID()
	writeClient := dbm.AllDbManger.RedisManger.GetMasterClient()
	lockSuccess, err := writeClient.SetNX(
		ctx, lockKey, lockVal, 5*time.Second, // 锁过期 5 秒（大于 DB 查询耗时）
	).Result()
	if err != nil {
		c.Error(&until.BusinessError{Code: 500, Message: "获取锁失败：" + err.Error()})
		return
	}

	// 5. 无锁且缓存未加载 → 发 MQ 异步查 DB
	if lockSuccess && status != "loading" {
		// 更新缓存状态为 loading（避免其他请求重复发 MQ）
		writeClient.HSet(
			ctx, cacheKey,
			"status", "loading",
			"keyword", keyword,
			"create_time", time.Now().Format("2006-01-02 15:04:05"),
		)
		writeClient.Expire(ctx, cacheKey, until.FuzzyCacheExpire)

		// 发 MQ 消息（携带关键词）
		msgBody := map[string]string{
			"keyword": keyword,
		}
		msgJSON, _ := json.Marshal(msgBody)
		publishCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := until.PublishPriorityMQ(publishCtx, until.FuzzyQueueName, msgJSON, priority); err != nil {
			c.Error(&until.BusinessError{Code: 500, Message: "发布模糊查询 MQ 消息失败 keyword:" + keyword + " error:" + err.Error()})
			return
			// MQ 失败，降级为同步查 DB
			// go syncFuzzyQueryDBAndUpdateCache(keyword)
		}
	}

	// 6. 存储请求状态（用于轮询）
	writeClient.HSet(
		ctx, reqStatusKey,
		"status", "pending",
		"keyword", keyword,
		"cache_key", cacheKey,
	)
	writeClient.Expire(ctx, reqStatusKey, 5*time.Minute)

	// 7. 无缓存，返回轮询提示
	c.JSON(http.StatusOK, until.Response{
		Code:    1,
		Message: "数据查询中，请轮询获取结果",
		Data: gin.H{
			"req_id":   reqID,
			"poll_url": fmt.Sprintf("/api/public/query/fuzzy/result?req_id=%s", reqID),
		},
	})
}

// GetFuzzyQueryResult 轮询模糊查询结果
// GET /api/public/query/fuzzy/result?req_id=xxx
func GetFuzzyQueryResult(c *gin.Context) {
	reqID := c.Query("req_id")
	if reqID == "" {
		c.Error(&until.BusinessError{Code: 400, Message: "参数错误：req_id不能为空"})
		return
	}

	reqStatusKey := fmt.Sprintf("%s%s", until.ResultCachePrefix, reqID)
	ctx := c.Request.Context()

	readClient, err := dbm.AllDbManger.RedisManger.GetSlaveClient()
	if err != nil {
		c.Error(&until.BusinessError{Code: 500, Message: "获取从节点客户端失败：" + err.Error()})
		return
	}
	// 1. 查请求状态
	statusMap, err := readClient.HGetAll(ctx, reqStatusKey).Result()
	if err != nil || len(statusMap) == 0 {
		c.Error(&until.BusinessError{Code: 404, Message: "请求不存在或已过期"})
		return
	}

	cacheKey := statusMap["cache_key"]

	// 2. 查缓存状态
	cacheData, err := readClient.HGetAll(ctx, cacheKey).Result()
	if err != nil || len(cacheData) == 0 {
		c.JSON(http.StatusOK, until.Response{
			Code:    1,
			Message: "数据查询中，建议 1 秒后再轮询",
			Data:    gin.H{"req_id": reqID, "progress": 50},
		})
		return
	}

	switch cacheData["status"] {
	case "loading":
		// 处理中
		c.JSON(http.StatusOK, until.Response{
			Code:    1,
			Message: "数据查询中，建议 1 秒后再轮询",
			Data:    gin.H{"req_id": reqID, "progress": 70},
		})
	case "failed":
		// 处理失败
		c.Error(&until.BusinessError{Code: 500, Message: "查询失败：" + cacheData["error_msg"]})
	case "ready":
		// 处理成功，返回完整结果
		var data interface{}
		json.Unmarshal([]byte(cacheData["data"]), &data)
		c.JSON(http.StatusOK, until.Response{
			Code:    0,
			Message: "获取成功",
			Data:    data,
		})
	}
}
