package handle

import (
	"net/http"

	"github/AHKLIC/Web/work/until"

	"github.com/gin-gonic/gin"
)

// 获取用户信息（需认证）
func UserProfileHandler(c *gin.Context) {
	// 从上下文获取 JWT 解析后的用户信息
	userID, _ := c.Get("userId")
	username, _ := c.Get("userName")

	c.JSON(http.StatusOK, until.Response{
		Code:    0,
		Message: "获取成功",
		Data:    gin.H{"userId": userID, "userName": username, "role": "admin"},
	})
}

// 示例业务接口（需认证）
func OperateHandler(c *gin.Context) {
	// 模拟业务逻辑（生产环境替换为真实业务）
	type OperateRequest struct {
		Action string `json:"action" binding:"required,oneof=add delete update"`
		Data   string `json:"data" binding:"required"`
	}
	var req OperateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(&until.BusinessError{Code: 400, Message: "参数错误：" + err.Error()})
		return
	}

	// 模拟业务错误（如权限不足）
	if req.Action == "delete" && req.Data == "admin" {
		c.Error(&until.BusinessError{Code: 403, Message: "禁止删除管理员数据"})
		return
	}

	c.JSON(http.StatusOK, until.Response{
		Code:    0,
		Message: "操作成功",
		Data:    gin.H{"action": req.Action, "result": "success"},
	})
}
