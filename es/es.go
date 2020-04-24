package es

import (
	"context"
	"github.com/olivere/elastic"
	"sync"
	"time"
)

var (
	Client *elastic.Client
	Once   sync.Once
)

var c *ServerConfig

type ServerConfig struct {
	IP       []string
	User     string
	Password string
}

func InitESConfig(ip []string, user string, password string) {
	c = &ServerConfig{IP: ip, User: user, Password: password}
}

func initESClient() {
	if c == nil {
		panic("not init es client")
	}
	var err error
	Client, err = elastic.NewClient(elastic.SetURL(c.IP...), elastic.SetBasicAuth(c.User, c.Password))
	if err != nil {
		panic(err)
	}
}

//创建 index
func PingNode() {
	start := time.Now()

	info, code, err := Client.Ping(c.IP[0]).Do(context.Background())
	if err != nil {
		fmt.Printf("ping es failed, err: %v", err)
	}

	duration := time.Since(start)
	fmt.Printf("cost time: %v\n", duration)
	fmt.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)
}

//校验 index 是否存在
func IndexExists(index ...string) bool {
	exists, err := client.IndexExists(index...).Do(context.Background())
	if err != nil {
		fmt.Printf("%v\n", err)
	}
	return exists
}

//创建 index
func CreateIndex(index, mapping string) bool {
	result, err := client.CreateIndex(index).BodyString(mapping).Do(context.Background())
	if err != nil {
		fmt.Printf("create index failed, err: %v\n", err)
	}
	return result.Acknowledged
}

//删除 index
func DelIndex(index ...string) bool {
	response, err := client.DeleteIndex(index...).Do(context.Background())
	if err != nil {
		fmt.Printf("delete index failed, err: %v\n", err)
	}
	return response.Acknowledged
}

//批量插入
func Batch(index string, type_ string, datas ...interface{}) {

	bulkRequest := client.Bulk()
	for _, data := range datas {
		doc := elastic.NewBulkIndexRequest().Index(index).Type(type_).Doc(data)
		bulkRequest = bulkRequest.Add(doc)
	}

	response, err := bulkRequest.Do(context.TODO())
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	failed := response.Failed()
	for _, v := range failed {
		fmt.Println(v.Error.Reason)
	}
	iter := len(failed)
	fmt.Printf("error: %v, %v\n", response.Errors, iter)
}

//获取指定 Id 的文档
func GetDoc(index, id string) []byte {
	temp := client.Get().Index(index).Id(id)
	get, err := temp.Do(context.Background())
	if err != nil {
		panic(err)
	}
	if get.Found {
		fmt.Printf("Got document %s in version %d from index %s, type %s\n", get.Id, get.Version, get.Index, get.Type)
	}
	source, err := get.Source.MarshalJSON()
	if err != nil {
		fmt.Printf("byte convert string failed, err: %v", err)
	}
	return source
}

//term 查询
func TermQuery(index, type_, fieldName, fieldValue string) *elastic.SearchResult {
	query := elastic.NewTermQuery(fieldName, fieldValue)
	//_ = elastic.NewQueryStringQuery(fieldValue) //关键字查询

	searchResult, err := client.Search().
		Index(index).Type(type_).
		Query(query).
		From(0).Size(10).
		Pretty(true).
		Do(context.Background())

	if err != nil {
		panic(err)
	}
	fmt.Printf("query cost %d millisecond.\n", searchResult.TookInMillis)

	return searchResult
}

func Search(index, type_ string) *elastic.SearchResult {
	boolQuery := elastic.NewBoolQuery()
	boolQuery.Must(elastic.NewMatchQuery("user", "Jame10"))
	boolQuery.Filter(elastic.NewRangeQuery("age").Gt("30"))
	searchResult, err := client.Search(index).
		Type(type_).Query(boolQuery).Pretty(true).Do(context.Background())
	if err != nil {
		panic(err)
	}

	return searchResult
}

func AggsSearch(index, type_ string) {

	minAgg := elastic.NewMinAggregation().Field("age")
	rangeAgg := elastic.NewRangeAggregation().Field("age").AddRange(0, 30).AddRange(30, 60).Gt(60)

	build := client.Search(index).Type(type_).Pretty(true)

	minResult, err := build.Aggregation("minAgg", minAgg).Do(context.Background())
	rangeResult, err := build.Aggregation("rangeAgg", rangeAgg).Do(context.Background())
	if err != nil {
		panic(err)
	}

	minAggRes, _ := minResult.Aggregations.Min("minAgg")
	fmt.Printf("min: %v\n", *minAggRes.Value)

	rangeAggRes, _ := rangeResult.Aggregations.Range("rangeAgg")
	for _, item := range rangeAggRes.Buckets {
		fmt.Printf("key: %s, value: %v\n", item.Key, item.DocCount)
	}

}

func GetAll(index, _type string) *elastic.SearchResult {
	var res *elastic.SearchResult
	var err error
	//取所有
	res, err = client.Search(index).Type(_type).Do(context.Background())
	if err != nil {
		fmt.Println(err)
	}
	return res
}
