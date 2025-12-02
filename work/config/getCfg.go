package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	ConfigPath string
	ConfigMu   sync.Mutex //protect update with config.json
)

// GlobalConfig 全局配置结构体
type GlobalConfig struct {
	MongoURL         string   `json:"mongo_url"`          //链接
	MongoDBNameData  string   `json:"mongodb_name_data"`  //数据库名
	MongoDBNameUsers string   `json:"mongodb_name_users"` //用户数据库名
	RedisSentinelArr []string `json:"redis_sentinelArr"`  // Redis 哨兵地址列表
	SourceList       []string `json:"source_list"`        // 数据源列表

}

var globalConfig GlobalConfig

// Init 初始化配置（读取config.json）
func Init(configPath string) error {
	ConfigPath = configPath
	// 处理配置文件路径（支持相对路径）
	absPath, err := filepath.Abs(configPath)
	if err != nil {
		return err
	}

	// 读取配置文件
	file, err := os.Open(absPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// 解析JSON
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&globalConfig); err != nil {
		return err
	}

	return nil
}

// GetGlobalConfig 获取全局配置
func GetGlobalConfig() GlobalConfig {
	return globalConfig
}

var (
	ShanghaiLoc *time.Location
)

func InitTimeZone() {
	// 初始化上海时区（提前处理错误，生产环境可优雅退出）
	var err error
	ShanghaiLoc, err = time.LoadLocation("Asia/Shanghai")
	if err != nil {
		panic(fmt.Sprintf("加载时区失败: %v", err))
	}
}
