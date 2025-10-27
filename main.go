package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/update"
	"github.com/elastic/go-elasticsearch/v8/typedapi/some"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/spf13/cast"
	"time"
)

func main() {
	// 连接ES

	cfg := elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"},
	}

	client, err := elasticsearch.NewTypedClient(cfg)
	if err != nil {
		fmt.Printf("NewTypeClient failed,err:%v\n", err)
		return
	}

	// 创建index
	//createIndex(client)
	//
	//// 创建document
	//indexDocument(client)

	// 查询document
	//getDocumentById(client, "1")

	searchDocument(client)

}

type Review struct {
	Id          int64     `json:"id"`
	UserId      int64     `json:"userId"`
	Score       uint8     `json:"score"`
	Content     string    `json:"content"`
	Tags        []Tag     `json:"tags"`
	Status      int       `json:"status"`
	PublishTime time.Time `json:"publishTime"`
}

type Tag struct {
	Code  int    `json:"code"`
	Title string `json:"title"`
}

// indexDocument 索引文档
func indexDocument(client *elasticsearch.TypedClient) {
	d1 := Review{
		Id:      1,
		UserId:  1499,
		Score:   5,
		Content: "这是一个好评!",
		Tags: []Tag{
			{1000, "好评"},
			{1100, "物有所值"},
			{9000, "有图"},
		},
		Status:      2,
		PublishTime: time.Now(),
	}
	resp, err := client.Index("my-review-1").Id(cast.ToString(d1.Id)).Document(d1).Do(context.Background())
	if err != nil {
		fmt.Printf("err:%v\n", err)
		return
	}
	fmt.Printf("resp:%v\n", resp)
}

func createIndex(client *elasticsearch.TypedClient) {
	resp, err := client.Indices.Create("my-review-1").Do(context.Background())
	if err != nil {
		fmt.Printf("CreateIndex failed,err:%v\n", err)
		return
	}
	fmt.Printf("CreateIndex succeed! ack:%v\n", resp.Acknowledged)
}

func getDocumentById(client *elasticsearch.TypedClient, id string) {
	resp, err := client.Get("my-review-1", id).Do(context.Background())
	if err != nil {
		fmt.Printf("Get failed,err:%v\n", err)
		return
	}
	fmt.Printf("resp:%s\n", resp.Source_)
}

// searchDocument 搜索所有文档
func searchDocument(client *elasticsearch.TypedClient) {
	// 搜索文档
	resp, err := client.Search().
		Index("my-review-1").
		Query(&types.Query{
			MatchAll: &types.MatchAllQuery{},
		}).
		Do(context.Background())
	if err != nil {
		fmt.Printf("search document failed, err:%v\n", err)
		return
	}
	fmt.Printf("total: %d\n", resp.Hits.Total.Value)
	// 遍历所有结果
	for _, hit := range resp.Hits.Hits {
		fmt.Printf("%s\n", hit.Source_)
	}
}

// searchDocument2 指定条件搜索文档
func searchDocument2(client *elasticsearch.TypedClient) {
	// 搜索content中包含好评的文档
	resp, err := client.Search().
		Index("my-review-1").
		Query(&types.Query{
			MatchPhrase: map[string]types.MatchPhraseQuery{
				"content": {Query: "好评"},
			},
		}).
		Do(context.Background())
	if err != nil {
		fmt.Printf("search document failed, err:%v\n", err)
		return
	}
	fmt.Printf("total: %d\n", resp.Hits.Total.Value)
	// 遍历所有结果
	for _, hit := range resp.Hits.Hits {
		fmt.Printf("%s\n", hit.Source_)
	}
}

// aggregationDemo 聚合
func aggregationDemo(client *elasticsearch.TypedClient) {
	avgScoreAgg, err := client.Search().
		Index("my-review-1").
		Request(
			&search.Request{
				Size: some.Int(0),
				Aggregations: map[string]types.Aggregations{
					"avg_score": { // 将所有文档的 score 的平均值聚合为 avg_score
						Avg: &types.AverageAggregation{
							Field: some.String("score"),
						},
					},
				},
			},
		).Do(context.Background())
	if err != nil {
		fmt.Printf("aggregation failed, err:%v\n", err)
		return
	}
	fmt.Printf("avgScore:%#v\n", avgScoreAgg.Aggregations["avg_score"])
}

// updateDocument 更新文档
func updateDocument(client *elasticsearch.TypedClient) {
	// 修改后的结构体变量
	d1 := Review{
		Id:      1,
		UserId:  147982601,
		Score:   5,
		Content: "这是一个修改后的好评！", // 有修改
		Tags: []Tag{ // 有修改
			{1000, "好评"},
			{9000, "有图"},
		},
		Status:      2,
		PublishTime: time.Now(),
	}

	resp, err := client.Update("my-review-1", "1").
		Doc(d1). // 使用结构体变量更新
		Do(context.Background())
	if err != nil {
		fmt.Printf("update document failed, err:%v\n", err)
		return
	}
	fmt.Printf("result:%v\n", resp.Result)
}

// updateDocument2 更新文档
func updateDocument2(client *elasticsearch.TypedClient) {
	// 修改后的JSON字符串
	str := `{
					"id":1,
					"userID":147982601,
					"score":5,
					"content":"这是一个二次修改后的好评！",
					"tags":[
						{
							"code":1000,
							"title":"好评"
						},
						{
							"code":9000,
							"title":"有图"
						}
					],
					"status":2,
					"publishDate":"2023-12-10T15:27:18.219385+08:00"
				}`
	// 直接使用JSON字符串更新
	resp, err := client.Update("my-review-1", "1").
		Request(&update.Request{
			Doc: json.RawMessage(str),
		}).
		Do(context.Background())
	if err != nil {
		fmt.Printf("update document failed, err:%v\n", err)
		return
	}
	fmt.Printf("result:%v\n", resp.Result)
}
