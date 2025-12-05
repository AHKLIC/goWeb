package main

import (
	"context"
	"fmt"
	mylog "github/AHKLIC/Web/slog"
	"github/AHKLIC/Web/work/dbm"
	"github/AHKLIC/Web/work/until"

	"github/AHKLIC/Web/work/handle"

	"github/AHKLIC/Web/work/config"

	"github.com/gin-gonic/gin"
)

// 路由注册（按功能分组）
func RegisterRoutes(r *gin.Engine) {

	// 公开路由组（无需认证）
	public := r.Group("/api/public")
	public.Use(until.PublicJWTMiddleware())
	{

		public.GET("/data/latest", handle.GetLatestCrawleData)
		public.GET("/query/fuzzy/search", handle.SubmitFuzzyQuery)
		// /api/public/query/fuzzy/result
		public.GET("/query/fuzzy/result", handle.GetFuzzyQueryResult)
	}
	public.POST("/login", handle.LoginHandler) // 登录接口（生成 JWT）
	public.GET("/health", handle.HealthCheck)  // 健康检查接口

	// 需认证路由组（添加 JWT 中间件）
	auth := r.Group("/api/auth")
	auth.Use(until.JWTMiddleware()) // 所有子路由都需要 JWT 认证
	{
		auth.GET("/profile", handle.UserProfileHandler) // 获取用户信息
		auth.POST("/operate", handle.OperateHandler)    // 示例业务接口
	}
}

func main() {

	mainCtx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		until.CloseMQ()
		dbm.AllDbManger.Close()
	}()
	var err error
	config.InitTimeZone() // 初始化时区
	// 初始化轮转日志（日志目录：./logs，前缀：crawler）
	rotatingWriter, logInitErr := mylog.InitRotatingLogger("./logs", "Web") //控制台+文件输出
	if logInitErr != nil {
		panic(fmt.Sprintf("init logger failed: %v", logInitErr))
	}
	defer rotatingWriter.Close() // 程序退出时关闭文件

	err = config.Init("config.json")
	if err != nil {
		panic(err)
	}
	dbm.AllDbManger, err = dbm.NewDbManger() //初始化数据库管理器
	if err != nil {
		panic(fmt.Sprintf("init db manger failed: %v", err))
	}
	until.InitMQ()
	until.StartMQConsumers(mainCtx)

	gin.SetMode(gin.DebugMode)
	r := gin.Default()

	r.Use(until.ErrorAndLogHandler())

	// 注册路由
	RegisterRoutes(r)

	// 启动服务
	fmt.Println("服务启动成功：http://localhost:8080")
	if err := r.Run(":8080"); err != nil {
		panic(fmt.Sprintf("服务启动失败：%v", err))
	}

}
