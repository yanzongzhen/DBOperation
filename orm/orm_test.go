package orm

import (
	"github.com/yanzongzhen/DBOperation/mysql"
	"github.com/yanzongzhen/Logger/logger"
	"testing"
	"time"
)

type Token struct {
	TokenID    string        `json:"token_id" orm:"token_id"`
	UserID     string        `json:"user_id" orm:"-"`
	Account    string        `json:"account" orm:"account"`
	DeviceType string        `json:"device_type" orm:"device_type"`
	DeviceID   string        `json:"device_id" orm:"device_id"`
	Type       int           `json:"type" orm:"type"`
	Expire     time.Time     `json:"expire" orm:"expire"`
	CreateTime time.Time     `json:"create_time" orm:"create_time"`
	Duration   time.Duration `json:"duration" orm:"duration"`
}

func TestQuery(t *testing.T) {
	logger.InitLogConfig(logger.DEBUG, true)
	//res := make([]map[string]interface{}, 0, 10)
	res := Token{}
	c := mysql.NewMySqlConfig("root", "11111111", "127.0.0.1", 3306, "ginkgo")

	err := Query(c, "select * from token", &res)
	logger.Debug(err)
	logger.Debug(res)

	//for _, t := range res {
	//	logger.Debug(t["account"])
	//}
}
