package until

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// 1. 全局配置
const (
	JWTSecret     = "your-secret-key-32bytes-long-1234" // 生产环境用环境变量读取，至少32位
	JWTExpireHour = 24 * 30                             // JWT 有效期（小时）
)

// 2. JWT 自定义声明（存储用户核心信息）
type JwtClaims struct {
	UserID               uint64 `json:"user_id"`
	Username             string `json:"username"`
	jwt.RegisteredClaims        // 内置标准声明（过期时间、签发时间等）
}

// 3. 统一错误响应结构体
type Response struct {
	Code    int         `json:"code"`    // 业务码
	Message string      `json:"message"` // 提示信息
	Data    interface{} `json:"data"`    // 响应数据（可选）
}

// 4. 自定义业务错误（支持错误码和消息）
type BusinessError struct {
	Code    int
	Message string
}

// 自定义日志
type LogLayout struct {
	Method    string `json:"method"`
	Path      string `json:"path"`
	Query     string `json:"query"`
	IP        string `json:"ip"`
	UserAgent string `json:"user_agent"`
	Error     string `json:"error,omitempty"`
	Cost      int64  `json:"cost"`
	Status    int    `json:"status"`
}

func (e *BusinessError) Error() string {
	return e.Message
}
func GenerateReqID() string {
	return uuid.NewString()
}

// 生成关键词的 MD5 哈希（作为缓存键核心）
func generateKeywordHash(keyword string) string {
	hash := md5.New()
	hash.Write([]byte(keyword))
	return string(hash.Sum(nil))
}

// 生成模糊查询缓存键
func GetFuzzyCacheKey(keyword string) string {
	return fmt.Sprintf("%s%s", FuzzyCachePrefix, generateKeywordHash(keyword))
}

// 生成模糊查询分布式锁键
func GetFuzzyLockKey(keyword string) string {
	return fmt.Sprintf("%s%s", FuzzyLockPrefix, generateKeywordHash(keyword))
}

// 5. 全局错误处理和日志输出中间件（捕获所有 panic 和错误）并输出日志
func ErrorAndLogHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				// 捕获 panic 错误
				c.JSON(http.StatusInternalServerError, Response{
					Code:    500,
					Message: fmt.Sprintf("服务器内部错误：%v", err),
				})
				c.Abort()
				return
			}
		}()
		action := c.Request.Method
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		c.Next() // 执行后续路由处理
		cost := time.Since(start).Milliseconds()
		layout := LogLayout{
			Method: action,
			Path:   path,
			Query:  query,
			IP:     c.ClientIP(), // 使用 ClientIP() 获取客户端IP[citation:2]
			Error:  "",
			Cost:   cost,
		}
		// 处理路由返回的错误（通过 c.Errors 获取）
		if len(c.Errors) > 0 {
			err := c.Errors.Last()
			var bizErr *BusinessError
			// 判断是否为自定义业务错误
			if errors.As(err.Err, &bizErr) {
				c.JSON(http.StatusOK, Response{
					Code:    bizErr.Code,
					Message: bizErr.Message,
				})
			} else {
				// 系统错误（如数据库、网络错误）
				c.JSON(http.StatusInternalServerError, Response{
					Code:    500,
					Message: "操作失败：" + err.Err.Error(),
				})
			}
			layout.Status = bizErr.Code
			layout.Error = err.Err.Error()
			go func() {
				logCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				layout.Status = c.Writer.Status()
				logData, _ := json.Marshal(layout)
				if err := PublishMQ(logCtx, AccessLogQueueName, logData); err != nil {
					slog.Error("publish access log msg failed: ", "error", err)
				}
			}()

			c.Abort()
		} else {
			go func() {
				logCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				layout.Status = c.Writer.Status()
				logData, _ := json.Marshal(layout)
				if err := PublishMQ(logCtx, AccessLogQueueName, logData); err != nil {
					slog.Error("publish access log msg failed: ", "error", err)
				}
			}()
		}
	}
}

// 6. JWT 生成工具（登录成功后调用）
func GenerateJWT(userID uint64, username string) (string, error) {
	// 构建 JWT 声明
	claims := JwtClaims{
		UserID:   userID,
		Username: username,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour * JWTExpireHour)), // 过期时间
			IssuedAt:  jwt.NewNumericDate(time.Now()),                                // 签发时间
			Issuer:    "gin-jwt-demo",                                                // 签发者
		},
	}

	// 生成 Token（使用 HS256 算法）
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(JWTSecret))
}

// 7. JWT 验证中间件（需要认证的路由添加此中间件）
func JWTMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 从请求头获取 Token（格式：Authorization: Bearer <token>）
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.Error(&BusinessError{Code: 401, Message: "未提供认证 Token"})
			c.Abort()
			return
		}

		// 解析 Token 格式
		var tokenStr string
		fmt.Sscanf(authHeader, "Bearer %s", &tokenStr)
		if tokenStr == "" {
			c.Error(&BusinessError{Code: 401, Message: "Token格式错误"})
			c.Abort()
			return
		}

		// 验证 Token
		token, err := jwt.ParseWithClaims(tokenStr, &JwtClaims{}, func(token *jwt.Token) (interface{}, error) {
			// 验证签名算法是否为 HS256
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("不支持的签名算法：%v", token.Header["alg"])
			}
			return []byte(JWTSecret), nil
		})

		// 处理验证错误
		if err != nil || !token.Valid {
			c.Error(&BusinessError{Code: 401, Message: "Token 无效或已过期"})
			c.Abort()
			return
		}

		// 提取 claims 并存入上下文（后续路由可通过 c.Get 获取）
		if claims, ok := token.Claims.(*JwtClaims); ok {
			c.Set("userId", claims.UserID)
			c.Set("userName", claims.Username)
		} else {
			c.Error(&BusinessError{Code: 401, Message: "Token 解析失败"})
			c.Abort()
			return
		}

		c.Next() // 验证通过，执行后续路由
	}
}
