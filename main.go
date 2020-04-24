package main

import (
	"database/sql"
	"fmt"
	"github.com/yanzongzhen/DBOperation/mysql"
	"github.com/yanzongzhen/Logger/logger"
	"sync"
	"time"
)

func Test1() int {
	fmt.Println("aaaaaaaaaaaa")
	return 0
}

func Test() int {
	defer func() {
		fmt.Println("defer")
	}()
	return Test1()
}

func main() {
	//Test()
	logger.InitLogConfig(logger.DEBUG, true)
	//c := redis.NewRedisConfig("10.110.9.234:6131", "redis", 0)
	//redis.Set(c, "111", "test", time.Minute)
	//logger.Debugln(redis.Exists(c, "1111"))
	dbConfig := mysql.NewMySqlConfigWithConnConfig("root", "123456a?", "172.22.16.139",
		3306, "icity", 2, 2, 3600)

	wg := new(sync.WaitGroup)
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(index int) {
			err := mysql.Query(dbConfig, "select id from cust_customer where mobile = ?", func(rows *sql.Rows) error {
				id := 0
				if rows.Next() {
					rows.Scan(&id)
				}
				logger.Debug(index, "======", id)
				return nil
			}, "18653558566")
			if err != nil {
				logger.Error(err)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	time.Sleep(time.Second * 20)
	logger.Debug("end")
	//time.Sleep(time.Second * 120)

	//etcd.Lock()
	//etcd.Lock(nil, nil, 1)

	//url := "172.22.16.188:2181"
	//
	//logger.InitLogConfig(logger.DEBUG, true)
	//exist, err := zk.Exists(url, "/IcityDataBus")
	//if err != nil {
	//	logger.Fatal(err)
	//}
	//
	//logger.Debug(exist)
	//
	//if !exist {
	//	err := zk.Create(url, "/IcityDataBus", nil)
	//
	//	if err != nil {
	//		logger.Fatal(err)
	//	}
	//} /*else {
	//	err := zk.Delete(url, "/IcityDataBus", 0)
	//	if err != nil {
	//		logger.Fatal(err)
	//	}
	//}*/
	//
	//nodes, err := zk.GetChildren(url, "/IcityDataBus")
	//if err != nil {
	//	logger.Fatal(exist)
	//}
	//
	//logger.Debug(nodes)
	//
	//children, err := zk.WatchChildren(url, "/IcityDataBus", func(eventType zk.WatchEventType, path string, version int32, newValue []byte) {
	//	logger.Debugln(eventType, path, version, newValue)
	//})
	//if err != nil {
	//	logger.Fatal(err)
	//}
	//
	//logger.Debug(children)
	//
	//err = zk.Create(url, "/IcityDataBus/topic5/nodes", []byte("111"))
	//if err != nil {
	//	logger.Fatal(err)
	//}
	//
	//err = zk.Create(url, "/IcityDataBus/topic6/nodes", []byte("222"))
	//if err != nil {
	//	logger.Fatal(err)
	//}

}
