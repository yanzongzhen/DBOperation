package redis

import (
	"github.com/yanzongzhen/Logger/logger"
	"testing"
)

func TestGet(t *testing.T) {
	logger.InitLogConfig(logger.DEBUG, true)
	config := Config{Url: "10.110.9.234:6131", Password: "redis", DB: 8}

	//for i := 0; i < 10; i++ {
	//	err := Get(&config, "convert_platform_0029916E44EF2A16B05D0177650F56CCdata", &data)
	//	if err != nil {
	//		t.Fatal(err)
	//	} else {
	//		logger.Debug(string(data))
	//	}
	//}

	err := TravelDB(&config, func(key []string) error {
		data := make([]byte, 0, 10)
		for _, k := range key {
			err := Get(&config, k, &data)
			if err != nil {
				//t.Fatal(err)
				return err
			}
			logger.Debug(string(data))
		}
		return nil
	}, 1000)

	logger.Error(err)
}
