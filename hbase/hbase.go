package hbase

import (
	"context"
	"errors"
	"fmt"
	"github.com/yanzongzhen/Logger/logger"
	"github.com/sirupsen/logrus"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"sync"
	"time"
)

var client gohbase.Client
var once sync.Once
var ErrorNotExist = errors.New("not found")

type Config struct {
	Url          string
	Table        string
	ColumnFamily string
	Column       string
}

func initClient(url string) {
	once.Do(func() {
		logrus.SetOutput(logger.GetLogger(logger.ERROR).Writer())
		client = gohbase.NewClient(url)
	})
}

func Get(c *Config, key string, deadline time.Duration) ([]byte, string, error) {
	initClient(c.Url)
	family := map[string][]string{c.ColumnFamily: {c.Column}}
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	getRequest, err := hrpc.NewGetStr(ctx, c.Table, key, hrpc.Families(family))
	if err != nil {
		return nil, "", fmt.Errorf("get hbase request failed:%v", err)
	}
	getRsp, err := client.Get(getRequest)
	if err != nil {
		return nil, "", fmt.Errorf("get hbase response failed:%v", err)
	}
	for _, cell := range getRsp.Cells {
		value := (*pb.Cell)(cell).GetValue()
		timeStamp := time.Unix(int64((*pb.Cell)(cell).GetTimestamp()/1e3), 0).Format("2006-01-02")
		return value, timeStamp, nil
	}
	return nil, "", ErrorNotExist
}

func GetTimeStamp(c *Config, key string, deadline time.Duration) (time.Time, error) {
	initClient(c.Url)
	family := map[string][]string{c.ColumnFamily: {c.Column}}
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	getRequest, err := hrpc.NewGetStr(ctx, c.Table, key, hrpc.Families(family))
	if err != nil {
		return time.Now(), fmt.Errorf("get hbase request failed:%v", err)
	}
	getRsp, err := client.Get(getRequest)
	if err != nil {
		return time.Now(), fmt.Errorf("get hbase response failed:%v", err)
	}
	for _, cell := range getRsp.Cells {
		timestamp := time.Unix(int64((*pb.Cell)(cell).GetTimestamp()/1e3), 0)
		return timestamp, nil
	}
	return time.Now(), ErrorNotExist
}

func Put(c *Config, key string, value []byte, deadline time.Duration) error {
	initClient(c.Url)
	values := map[string]map[string][]byte{c.ColumnFamily: {c.Column: value}}
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	putRequest, err := hrpc.NewPutStr(ctx, c.Table, key, values)
	if err != nil {
		return fmt.Errorf("put hbase request failed:%v", err)
	}
	_, err = client.Put(putRequest)
	if err != nil {
		return fmt.Errorf("put hbase failed:%v", err)
	}
	return nil
}

func Delete(c *Config, key string, deadline time.Duration) error {
	initClient(c.Url)
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	deleteRequest, err := hrpc.NewDelStr(ctx, c.Table, key, nil)
	if err != nil {
		return fmt.Errorf("get delete request failed:%v", err)
	}
	_, err = client.Delete(deleteRequest)
	if err != nil {
		return fmt.Errorf("delete hbase data failed:%v", err)
	}
	return nil
}
