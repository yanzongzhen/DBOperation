package redis

import (
	"errors"
	"github.com/yanzongzhen/Logger/logger"
	"github.com/yanzongzhen/utils/crypto"
	redis2 "github.com/go-redis/redis"
	"reflect"
	"strconv"
	"sync"
	"time"
)

func init() {
	clientMap = make(map[string]*redisConn)
	lock = &sync.RWMutex{}
}

const pingInterval = 20 * time.Second

var clientMap map[string]*redisConn

var lock *sync.RWMutex

var ErrorNotExist = errors.New("not found")

type Config struct {
	Url      string `json:"redis_url"`
	Password string `json:"password"`
	DB       int    `json:"redis_db"`
}

type redisConn struct {
	err     error
	client  *redis2.Client
	stop    chan int
	diaLock *sync.Mutex
	config  *Config
}

func newConnection(config *Config) *redisConn {
	c := &redisConn{}
	c.err = errors.New("first")
	c.config = config
	c.diaLock = &sync.Mutex{}
	c.stop = make(chan int)
	return c
}

func (c *redisConn) dial() error {
	c.diaLock.Lock()
	defer c.diaLock.Unlock()
	if c.err == nil {
		return nil
	}
	redisClient := redis2.NewClient(&redis2.Options{
		Addr:         c.config.Url,
		Password:     c.config.Password,
		DB:           c.config.DB,
		DialTimeout:  time.Second * 5,
		ReadTimeout:  time.Second * 5,
		WriteTimeout: time.Second * 5,
		MaxRetries:   3,
	})

	c.err = redisClient.Ping().Err()
	c.client = redisClient
	return c.err
}

func (c *redisConn) ping() {
	for {
		time.Sleep(pingInterval)
		select {
		case <-c.stop:
			return
		default:
			break
		}

		if c.err == nil {
			c.err = c.client.Ping().Err()
		} else {
			if c.client != nil {
				_ = c.client.Close()
			}
			c.err = c.dial()
		}
	}
}

func (c *redisConn) disConnect() {
	close(c.stop)
	if c.client != nil {
		_ = c.client.Close()
	}
}

func NewRedisConfig(url string, password string, db int) *Config {
	return &Config{
		Url:      url,
		Password: password,
		DB:       db,
	}
}

func (config *Config) getConfigStr() string {
	return crypto.MD5(config.Url + config.Password + strconv.Itoa(config.DB))
}

func initRedisClient(config *Config) *redis2.Client {
	//lock.Lock()

	lock.RLock()
	c, ok := clientMap[config.getConfigStr()]
	lock.RUnlock()
	if ok {
		if c.err == nil {
			return c.client
		} else {
			c.disConnect()
			lock.Lock()
			delete(clientMap, config.getConfigStr())
			lock.Unlock()
			return nil
		}
	} else {
		lock.Lock()
		defer lock.Unlock()
		if c, ok := clientMap[config.getConfigStr()]; ok {
			if c.err == nil {
				return c.client
			}
			return nil
		} else {
			c := newConnection(config)
			err := c.dial()
			if err != nil {
				return nil
			}
			go c.ping()
			clientMap[config.getConfigStr()] = c
			return c.client
		}
	}
}

func Set(config *Config, key string, value interface{}, expireTime time.Duration) error {
	client := initRedisClient(config)
	if client == nil {
		return errors.New("connect redis error")
	}
	return client.Set(key, value, expireTime).Err()
}

func HSet(config *Config, key string, field string, value interface{}) error {
	client := initRedisClient(config)
	if client == nil {
		return errors.New("connect redis error")
	}
	return client.HSet(key, field, value).Err()
}

func HMSet(config *Config, key string, fields map[string]interface{}) error {
	client := initRedisClient(config)
	if client == nil {
		return errors.New("connect redis error")
	}
	return client.HMSet(key, fields).Err()
}

func HSetNX(config *Config, key string, field string, value interface{}) error {
	client := initRedisClient(config)
	if client == nil {
		return errors.New("connect redis error")
	}
	return client.HSetNX(key, field, value).Err()
}

func SetNX(config *Config, key string, value interface{}, expireTime time.Duration) (bool, error) {
	client := initRedisClient(config)
	if client == nil {
		return false, errors.New("connect redis error")
	}
	res := client.SetNX(key, value, expireTime)
	isOk, err := res.Result()
	if err != nil {
		logger.Error(err)
		logger.Error(isOk)
		return false, err
	}
	if !isOk {
		return false, nil
	}
	return true, nil
}

func Delete(config *Config, key ...string) error {
	client := initRedisClient(config)
	if client == nil {
		return errors.New("connect redis error")
	}
	return client.Del(key...).Err()
}

func Get(config *Config, key string, value interface{}) error {
	client := initRedisClient(config)
	if client == nil {
		return errors.New("connect redis error")
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		logger.Errorln("value必须为指针类型")
		return errors.New("value must be pointer")
	}
	res := client.Get(key)
	if err := res.Err(); err == nil {
		return res.Scan(value)
	} else {
		if err == redis2.Nil {
			return ErrorNotExist
		}
		return err
	}
}

func HGet(config *Config, key string, field string, value interface{}) error {
	client := initRedisClient(config)
	if client == nil {
		return errors.New("connect redis error")
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		logger.Errorln("value必须为指针类型")
		return errors.New("value must be pointer")
	}
	res := client.HGet(key, field)
	if err := res.Err(); err == nil {
		return res.Scan(value)
	} else {
		if err == redis2.Nil {
			return ErrorNotExist
		}
		return err
	}
}

func IncrNum(config *Config, key string) (int64, error) {
	client := initRedisClient(config)
	if client == nil {
		return -1, errors.New("connect redis error")
	}
	res := client.Incr(key)
	if err := res.Err(); err == nil {
		return res.Val(), nil
	} else {
		if err == redis2.Nil {
			return -1, ErrorNotExist
		}
		return -1, err
	}
}

func IncrBYNum(config *Config, key string, num int) (int64, error) {
	client := initRedisClient(config)
	if client == nil {
		return -1, errors.New("connect redis error")
	}
	res := client.IncrBy(key, int64(num))
	if err := res.Err(); err == nil {
		return res.Val(), nil
	} else {
		if err == redis2.Nil {
			return -1, ErrorNotExist
		}
		return -1, err
	}
}

func DecrByNum(config *Config, key string, num int) (int64, error) {
	client := initRedisClient(config)
	if client == nil {
		return -1, errors.New("connect redis error")
	}
	res := client.DecrBy(key, int64(num))
	if err := res.Err(); err == nil {
		return res.Val(), nil
	} else {
		if err == redis2.Nil {
			return -1, ErrorNotExist
		}
		return -1, err
	}
}

//func De() {
//	client := initRedisClient(config, 0)
//
//}

func DecrNum(config *Config, key string) (int64, error) {
	client := initRedisClient(config)
	if client == nil {
		return -1, errors.New("connect redis error")
	}
	res := client.Decr(key)
	if res.Err() == nil {
		return res.Val(), nil
	}
	if res.Err() == redis2.Nil {
		return -1, ErrorNotExist
	}
	return -1, res.Err()
}

type TravelKeysFunc func(key []string) error

func TravelDB(config *Config, tFunc TravelKeysFunc, count int64) error {
	client := initRedisClient(config)

	if client == nil {
		return errors.New("connect redis error")
	}
	var cursor uint64 = 0
	for {
		keysScan := client.Scan(cursor, "", count)
		keys, newCursor, err := keysScan.Result()
		if err != nil {
			return err
		}

		err = tFunc(keys)
		if err != nil {
			return err
		}

		if newCursor == 0 {
			break
		}
		cursor = newCursor
	}
	return nil
}

func SMembers(config *Config, key string) ([]string, error) {
	client := initRedisClient(config)

	if client == nil {
		return nil, errors.New("connect redis error")
	}

	memberCmd := client.SMembers(key)
	res, err := memberCmd.Result()
	if err != nil {
		return nil, err
	}
	return res, err
}

func TTL(config *Config, key string) (time.Duration, error) {
	client := initRedisClient(config)
	if client == nil {
		return -1, errors.New("connect redis error")
	}

	res := client.TTL(key)
	if err := res.Err(); err == nil {
		return res.Result()
	} else {
		if err == redis2.Nil {
			return -1, ErrorNotExist
		}
		return -1, err
	}
}

func Exists(config *Config, key ...string) (bool, error) {
	client := initRedisClient(config)
	if client == nil {
		return false, errors.New("connect redis error")
	}

	res := client.Exists(key...)
	if err := res.Err(); err == nil {
		r, _ := res.Result()
		return r == 1, nil
	} else {
		if err == redis2.Nil {
			return false, nil
		}
		return false, err
	}
}

func HIncr(config *Config, key string, field string, num int64) (int64, error) {
	client := initRedisClient(config)
	if client == nil {
		return -1, errors.New("connect redis error")
	}
	res := client.HIncrBy(key, field, num)
	if err := res.Err(); err == nil {
		return res.Val(), nil
	} else {
		if err == redis2.Nil {
			return -1, ErrorNotExist
		}
		return -1, err
	}
}
