package dbm

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type MongoManger struct {
	mongoClient      *mongo.Client // MongoDB 客户端
	mongodbDatasName string        //数据库爬取数据库
	mongodbUsersName string        //数据库用户库
}

func (m *MongoManger) GetMongoClient() *mongo.Client {
	return m.mongoClient
}

// mongo数据库查询返回模糊查询文档
func (m *MongoManger) GetMongoDataFuzzyByKeyword(keyword string) (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dbInstance := m.mongoClient.Database(m.mongodbDatasName)
	collectionNames, err := dbInstance.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("获取集合列表失败: %w", err)
	}

	// 并行查询通道
	resultChan := make(chan []bson.M, len(collectionNames))
	errChan := make(chan error, len(collectionNames))
	var wg sync.WaitGroup

	// 为每个集合启动goroutine进行并行聚合查询
	for _, collName := range collectionNames {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			coll := dbInstance.Collection(name)
			// 查询单个集合，返回 []bson.M
			results, err := querySingleCollection(ctx, coll, keyword)
			if err != nil {
				errChan <- fmt.Errorf("集合 %s 查询失败: %w", name, err)
				return
			}
			resultChan <- results
		}(collName)
	}

	// 等待所有goroutine完成
	wg.Wait()
	close(resultChan)
	close(errChan)

	// 检查并行查询中的错误（这里选择记录但继续，你也可以选择立即返回错误）
	for err := range errChan {
		// 记录到日志，不中断主流程
		slog.Error("并行查询时发生错误", "error", err)
	}

	// 合并所有集合的查询结果
	var allResults []bson.M
	for results := range resultChan {
		allResults = append(allResults, results...)
	}

	// 全局去重、排序和限制
	finalResults := deduplicateAndSort(allResults, 50)
	return finalResults, nil
}

// querySingleCollection 查询单个集合，返回泛化的文档列表
func querySingleCollection(ctx context.Context, coll *mongo.Collection, keyword string) ([]bson.M, error) {
	// MongoDB聚合管道定义
	pipeline := mongo.Pipeline{
		// 1. 匹配：hotitem.title 字段包含 keyword（不区分大小写）
		{
			{Key: "$match", Value: bson.D{
				{Key: "hotitem.title", Value: bson.D{
					{Key: "$regex", Value: keyword},
					{Key: "$options", Value: "i"},
				}},
			}},
		},
		// 2. 按标题分组去重，并保留每组中crawledat最新的文档
		{
			{Key: "$sort", Value: bson.D{
				{Key: "hotitem.crawledat", Value: -1},
			}},
		},
		{
			{Key: "$group", Value: bson.D{
				{Key: "_id", Value: "$hotitem.title"},
				// 取排序后的第一条，即最新的文档
				{Key: "doc", Value: bson.D{
					{Key: "$first", Value: "$$ROOT"},
				}},
			}},
		},
		// 3. 将分组后保留的文档替换为根
		{
			{Key: "$replaceRoot", Value: bson.D{
				{Key: "newRoot", Value: "$doc"},
			}},
		},
		// 4. 再次按时间倒序排序（针对去重后的结果）
		{
			{Key: "$sort", Value: bson.D{
				{Key: "hotitem.crawledat", Value: -1},
			}},
		},
		// 5. 限制单个集合返回的文档数量
		{
			{Key: "$limit", Value: 100},
		},
	}
	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	// 解码为通用的 bson.M 列表
	var results []bson.M
	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}
	return results, nil
}

// deduplicateAndSort 对泛化文档进行去重、排序和数量限制
func deduplicateAndSort(docs []bson.M, limit int) []bson.M {
	// 用于去重的map，key为title，value为文档
	seen := make(map[string]bson.M)

	for _, doc := range docs {
		// 从嵌套的 hotitem 字段中提取 title 和 crawledat
		hotitem, ok := doc["hotitem"].(bson.M)
		if !ok {
			continue // 如果文档结构不符合预期，跳过
		}

		title, titleOk := hotitem["title"].(string)
		crawledAtRaw, timeOk := hotitem["crawledat"].(primitive.DateTime) // MongoDB中的时间类型

		if !titleOk || !timeOk {
			continue // 如果关键字段缺失或类型不对，跳过
		}

		crawledAt := time.Unix(int64(crawledAtRaw)/1000, 0) // 转换为Go的time.Time

		// 去重逻辑：如果该标题还未出现，或新文档的爬取时间更晚，则更新
		existingDoc, exists := seen[title]
		if !exists {
			seen[title] = doc
			continue
		}

		// 比较时间
		existingHotitem := existingDoc["hotitem"].(bson.M)
		existingCrawledAtRaw := existingHotitem["crawledat"].(primitive.DateTime)
		existingCrawledAt := time.Unix(int64(existingCrawledAtRaw)/1000, 0)

		if crawledAt.After(existingCrawledAt) {
			seen[title] = doc
		}
	}

	// 转换为切片，准备排序
	uniqueDocs := make([]bson.M, 0, len(seen))
	for _, doc := range seen {
		uniqueDocs = append(uniqueDocs, doc)
	}

	// 按 hotitem.crawledat 倒序排序
	sort.Slice(uniqueDocs, func(i, j int) bool {
		hotitemI := uniqueDocs[i]["hotitem"].(bson.M)
		hotitemJ := uniqueDocs[j]["hotitem"].(bson.M)
		timeI := hotitemI["crawledat"].(primitive.DateTime)
		timeJ := hotitemJ["crawledat"].(primitive.DateTime)
		return timeI > timeJ // primitive.DateTime 可直接比较大小
	})

	// 限制最终返回数量
	if len(uniqueDocs) > limit {
		return uniqueDocs[:limit]
	}
	return uniqueDocs
}
