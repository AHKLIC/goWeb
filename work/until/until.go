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

// 全局配置
const (
	JWTSecret     = "your-secret-key-32bytes-long-1234" // 生产环境用环境变量读取，至少32位
	JWTExpireHour = 24 * 30                             // JWT 有效期（小时）
)

// JWT 自定义声明（存储用户核心信息）
type JwtClaims struct {
	UserID               uint64 `json:"user_id"`
	Username             string `json:"username"`
	jwt.RegisteredClaims        // 内置标准声明（过期时间、签发时间等）
}

// 统一错误响应结构体
type Response struct {
	Code    int         `json:"code"`    // 业务码
	Message string      `json:"message"` // 提示信息
	Data    interface{} `json:"data"`    // 响应数据（可选）
}

// 自定义业务错误（支持错误码和消息）
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

// 全局常量（区分用户类型，便于后续使用）
const (
	UserTypeVIP    = "vip"    // VIP 用户（Token 校验成功）
	UserTypeNormal = "normal" // 普通用户（无 Token 或 Token 无效）
)

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

// 全局错误处理和日志输出中间件（捕获所有 panic 和错误）并输出日志
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
				layout.Status = bizErr.Code
				if bizErr.Code != 601 {
					c.JSON(http.StatusOK, Response{
						Code:    bizErr.Code,
						Message: bizErr.Message,
					})

				}

			} else {
				// 系统错误（如数据库、网络错误）
				c.JSON(http.StatusInternalServerError, Response{
					Code:    500,
					Message: "操作失败：" + err.Err.Error(),
				})
				layout.Status = 500
			}
			layout.Error = err.Err.Error()
			go func() {
				logCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
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

// JWT 生成工具（登录成功后调用）
func GenerateJWT(userID uint64, username string) (string, error) {
	// 构建 JWT 声明
	claims := JwtClaims{
		UserID:   userID,
		Username: username,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour * JWTExpireHour)), // 过期时间
			IssuedAt:  jwt.NewNumericDate(time.Now()),                                // 签发时间
			Issuer:    "AHKLIC-GO-WEB",                                               // 签发者
		},
	}

	// 生成 Token（使用 HS256 算法）
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(JWTSecret))
}

// JWT 验证中间件（需要认证的路由添加此中间件）
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

// PublicJWTMiddleware 软判断 JWT 中间件
// 逻辑：
// 1. 无 Authorization 头 → 普通用户（不报错，继续执行）
// 2. 有 Authorization 头 → 校验 Token：
//   - 校验成功 → VIP 用户
//   - 校验失败 → 记录错误日志 → 普通用户（不中断流程）
func PublicJWTMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 初始化用户类型为普通用户（默认）
		userType := UserTypeNormal
		var userId uint64
		var userName string

		// 1. 获取 Authorization 头
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			// 无 Token → 普通用户，直接向下执行（不记录错误）
			c.Set("user_type", userType)
			c.Next()
			return
		}

		// 2. 有 Token，解析格式（Bearer <token>）
		var tokenStr string
		_, err := fmt.Sscanf(authHeader, "Bearer %s", &tokenStr)
		if err != nil || tokenStr == "" {
			// Token 格式错误 → 记录错误日志 → 普通用户
			//601->401但是601不返回error给客户端
			c.Error(&BusinessError{Code: 601, Message: "Token格式错误,降级为普通用户"})
			c.Set("user_type", userType)
			c.Next()
			return
		}

		// 3. 验证 Token 签名和有效性
		token, err := jwt.ParseWithClaims(tokenStr, &JwtClaims{}, func(token *jwt.Token) (interface{}, error) {
			// 验证签名算法是否为 HS256（与你的 Token 生成逻辑一致）
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("不支持的签名算法：%v", token.Header["alg"])
			}
			return []byte(JWTSecret), nil // JWTSecret 是你的签名密钥（保持原有）
		})

		// 4. 处理 Token 校验结果
		if err != nil || !token.Valid {
			// Token 无效/过期 → 记录错误日志 → 普通用户
			c.Error(&BusinessError{Code: 601, Message: "Token 无效或已过期,降级为普通用户"})
			c.Set("user_type", userType)
			c.Next()
			return
		}

		// 5. Token 校验成功 → 提取用户信息，标记为 VIP 用户
		if claims, ok := token.Claims.(*JwtClaims); ok {
			userType = UserTypeVIP
			userId = claims.UserID
			userName = claims.Username
		} else {
			// Token 解析失败（极少发生）→ 记录错误 → 普通用户
			c.Error(&BusinessError{Code: 601, Message: "Token 解析失败,降级为普通用户"})
			c.Set("user_type", userType)
			c.Next()
			return
		}

		// 6. 将用户信息存入 Gin 上下文（后续接口可通过 c.Get 获取）
		c.Set("user_type", userType)
		c.Set("userId", userId)
		c.Set("userName", userName)

		// 7. 继续执行后续路由
		c.Next()
	}
}
