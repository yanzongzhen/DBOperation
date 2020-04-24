package es

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

type PushInfo struct {
	AppId         string    `json:"app_id"`
	Uuid          string    `json:"uuid"`
	DataFromCache bool      `json:"data_from_cache"`
	RequestTime   string    `json:"request_time"`
	Name          string    `json:"name"`
	RequestName   string    `json:"request_name"`
	Status        bool      `json:"status"`
	ResponseTime  int       `json:"response_time"`
	ClientArgs    []Arg     `json:"client_args"`
	FinalHeader   []Arg     `json:"final_header"`
	FinalArgs     string    `json:"final_args"`
	ErrorArray    []Error   `json:"error_array"`
	Nginx         NginxInfo `json:"nginx"`
	Hash          string    `json:"hash"`
}

type Arg struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Error struct {
	ErrorCode string `json:"error_code"`
	Type      string `json:"type"`
	Msg       string `json:"msg"`
	FieldName string `json:"field_name"`
}

type NginxInfo struct {
	UUID         string `json:"uuid"`
	Timestamp    string `json:"@timestamp"`
	Host         string `json:"host"`
	Method       string `json:"method"`
	URL          string `json:"url"`
	ClientIP     string `json:"clientip"`
	Protocol     string `json:"protocol"`
	UserAgent    string `json:"user_agent"`
	Referer      string `json:"referer"`
	HttpHost     string `json:"http_host"`
	Status       string `json:"status"`
	CacheStatus  string `json:"cache_status"`
	UpstreamAddr string `json:"upstream_addr"`
	SourceIp     string `json:"source_ip"`
}

var mapping = `{
    "settings": {
        "number_of_shards": 3,
        "number_of_replicas": 1,
        "analysis": {
            "normalizer": {
                "my_normalizer": {
                    "type": "custom",
                    "char_filter": [],
                    "filter": [
                        "lowercase",
                        "asciifolding"
                    ]
                }
            }
        }
    },
    "mappings": {
        "dpp": {
            "properties": {
                "app_id": {
                    "type": "keyword"
                },
                "uuid": {
                    "type": "keyword"
                },
                "name": {
                    "type": "text"
                },
                "request_name": {
                    "type": "keyword",
                    "normalizer": "my_normalizer"
                },
                "hash": {
                    "type": "keyword"
                },
                "status": {
                    "type": "boolean"
                },
                "response_time": {
                    "type": "integer"
                },
                "data_from_cache": {
                    "type": "boolean"
                },
                "request_time": {
                    "type": "date",
  					"format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                },
                "client_args": {
                    "type": "nested"
                },
                "final_header": {
                    "type": "nested"
                },
                "final_args": {
                    "type": "text"
                },
                "error_array": {
                    "type": "nested"
                },
                "nginx": {
                    "properties": {
                        "uuid": {
                            "type": "keyword"
                        },
                        "@timestamp": {
                            "type": "keyword"
                        },
                        "host": {
                            "type": "keyword"
                        },
                        "method": {
                            "type": "keyword"
                        },
                        "url": {
                            "type": "text"
                        },
                        "clientip": {
                            "type": "keyword"
                        },
                        "protocol": {
                            "type": "keyword"
                        },
                        "user_agent": {
                            "type": "text"
                        },
                        "referer": {
                            "type": "keyword"
                        },
                        "http_host": {
                            "type": "keyword"
                        },
                        "status": {
                            "type": "keyword"
                        },
                        "cache_status": {
                            "type": "keyword"
                        },
                        "upstream_addr": {
                            "type": "keyword"
                        },
                        "source_ip": {
                            "type": "keyword"
                        }
                    }
                }
            }
        }
    }
}`

var index = "trace_dpp"
var _type = "dpp"

func TestPingNode(t *testing.T) {
	PingNode()
}

func TestIndexExists(t *testing.T) {
	result := IndexExists(index)
	fmt.Println("all index exists: ", result)
}

func TestDeleteIndex(t *testing.T) {
	result := DelIndex(index)
	fmt.Println("all index deleted: ", result)
}

func TestCreateIndex(t *testing.T) {
	result := CreateIndex(index, mapping)
	fmt.Println("mapping created: ", result)
}

func TestBatch(t *testing.T) {
	Info := PushInfo{
		AppId:         "fuckzongzhen",
		Uuid:          "123456",
		DataFromCache: false,
		RequestTime:   "2006-01-02 15:04:05",
		Name:          "test",
		RequestName:   "HellO",
		Status:        false,
		ResponseTime:  30000,
		Hash:          "fuckzzzzzzzzzzzzzz",
		ClientArgs: []Arg{
			{
				Key:   "name",
				Value: "张三",
			},
			{
				Key:   "age",
				Value: "13",
			},
		},
		FinalHeader: []Arg{
			{
				Key:   "content-type",
				Value: "json",
			},
		},
		FinalArgs: `{"region": "US","manager": {"age": 30,"name": {"first": "John","last": "Smith"}}}`,
		ErrorArray: []Error{
			{
				ErrorCode: "3000",
				Type:      "requestError",
				Msg:       "no route to host",
				FieldName: "name",
			},
		},
		Nginx: NginxInfo{
			UUID:         "12314141543523436",
			Timestamp:    "2019-12-04T19:18:16+08:00",
			Host:         "127.0.0.1:80",
			Method:       "GET",
			URL:          "/index.html",
			ClientIP:     "127.0.0.1",
			Protocol:     "HTTP/1.1",
			UserAgent:    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
			Referer:      "-",
			HttpHost:     "127.0.0.1",
			Status:       "304",
			CacheStatus:  "-",
			UpstreamAddr: "-",
			SourceIp:     "fuckzzzzzzzz",
		},
	}
	Batch(index, _type, Info)
}

func TestGetDoc(t *testing.T) {
	var info PushInfo
	data := GetDoc(index, "0")
	if err := json.Unmarshal(data, &info); err == nil {
		fmt.Printf("data: %v\n", info)
	}
}

//func TestTermQuery(t *testing.T) {
//	var tweet Tweet
//	result := TermQuery("twitter", "doc", "user", "Take Two")
//	//获得数据, 方法一
//	for _, item := range result.Each(reflect.TypeOf(tweet)) {
//		if t, ok := item.(Tweet); ok {
//			fmt.Printf("tweet : %v\n", t)
//		}
//	}
//	//获得数据, 方法二
//	fmt.Println("num of raws: ", result.Hits.TotalHits)
//	if result.Hits.TotalHits > 0 {
//		for _, hit := range result.Hits.Hits {
//			err := json.Unmarshal(*hit.Source, &tweet)
//			if err != nil {
//				fmt.Printf("source convert json failed, err: %v\n", err)
//			}
//			fmt.Printf("data: %v\n", tweet)
//		}
//	}
//}

func TestSearch(t *testing.T) {
	result := Search(index, _type)
	var info PushInfo
	for _, item := range result.Each(reflect.TypeOf(info)) {
		if t, ok := item.(PushInfo); ok {
			fmt.Printf("info : %v\n", t)
		}
	}
}

func TestGetAll(t *testing.T) {
	result := GetAll(index, _type)
	var info PushInfo
	for _, item := range result.Each(reflect.TypeOf(info)) {
		fmt.Println(item)
		if t, ok := item.(PushInfo); ok {
			fmt.Printf("info : %v\n", t)
		}
	}
}

func TestAggsSearch(t *testing.T) {
	AggsSearch(index, _type)
}
