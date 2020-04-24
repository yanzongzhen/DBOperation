package nsq

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/yanzongzhen/Logger/logger"
	nsq "github.com/nsqio/go-nsq"
	"io/ioutil"
	"math/rand"
	"net/http"
	"time"
)

var producer *nsq.Producer
var nsqHttpAddrs = []string{"10.72.124.102:3000", "10.72.124.102:4000", "10.72.124.102:5000"}
var nsqTcpAddrs = []string{"10.72.124.102:3001", "10.72.124.102:4001", "10.72.124.102:5001"}

func InitNsqProducer() error {
	var err error
	logger.Debugf("初始化NSQ配置:%v", nsqTcpAddrs)
	config := nsq.NewConfig()
	config.DialTimeout = 1 * time.Second
	config.DefaultRequeueDelay = 40 * time.Second
	config.LookupdPollInterval = 4 * time.Second
	config.HeartbeatInterval = 10 * time.Second
	id := rand.Intn(3)
	producer, err = nsq.NewProducer(nsqTcpAddrs[id], config)
	if err != nil {
		logger.Errorf("error: %v", err)
		return err
	}
	return nil
}

func PushNsqForMonitor(topic string, data []byte) {
	err := producer.Publish(topic, data)
	if err != nil {
		logger.Errorf("监控消息发送失败---%v", err)
	} else {
		logger.Debugf("监控消息发送成功")
	}
}

func Publish(topic string, message string) error {
	if producer != nil {
		if message == "" {
			fmt.Println("the message should not be nil")
			return errors.New("empty message")
		}
		err := producer.Publish(topic, []byte(message))
		return err
	} else {
		err := InitNsqProducer()
		if err != nil {
			return err
		} else {
			err := producer.Publish(topic, []byte(message))
			if err != nil {
				return err
			} else {
				return nil
			}
		}
	}
}

func PublishWithHttp(endpoint string, topic string, message string) ([]byte, error) {
	buf := bytes.NewReader([]byte(message))
	resp, err := http.Post("http://"+endpoint+"/pub?topic="+topic, "application/json", buf)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	return body, nil
}

func CreateTopic(endpoint string, topic string) ([]byte, error) {

	resp, err := http.Post("http://"+endpoint+"/topic/create?topic="+topic, "application/json", nil)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	return body, nil
}

func DeleteTopic(endpoint string, topic string) ([]byte, error) {

	resp, err := http.Post("http://"+endpoint+"/topic/delete?topic="+topic, "application/json", nil)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	return body, nil
}

func EmptyTopic(endpoint string, topic string) ([]byte, error) {

	resp, err := http.Post("http://"+endpoint+"/topic/empty?topic="+topic, "application/json", nil)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	return body, nil
}

func PauseTopic(endpoint string, topic string) ([]byte, error) {

	resp, err := http.Post("http://"+endpoint+"/topic/pause?topic="+topic, "application/json", nil)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	return body, nil
}

func UnpauseTopic(endpoint string, topic string) ([]byte, error) {

	resp, err := http.Post("http://"+endpoint+"/topic/unpause?topic="+topic, "application/json", nil)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	return body, nil
}

func CreateChannel(endpoint string, topic string, channel string) ([]byte, error) {
	resp, err := http.Post("http://"+endpoint+"/channel/create?topic="+topic+"&channel="+channel, "application/json", nil)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	return body, nil
}

func DeleteChannel(endpoint string, topic string, channel string) ([]byte, error) {
	resp, err := http.Post("http://"+endpoint+"/channel/delete?topic="+topic+"&channel="+channel, "application/json", nil)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	return body, nil
}

func EmptyChannel(endpoint string, topic string, channel string) ([]byte, error) {
	resp, err := http.Post("http://"+endpoint+"/channel/empty?topic="+topic+"&channel="+channel, "application/json", nil)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	return body, nil
}

func PauseChannel(endpoint string, topic string, channel string) ([]byte, error) {
	resp, err := http.Post("http://"+endpoint+"/channel/pause?topic="+topic+"&channel="+channel, "application/json", nil)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	return body, nil
}

func UnpauseChannel(endpoint string, topic string, channel string) ([]byte, error) {
	resp, err := http.Post("http://"+endpoint+"/channel/unpause?topic="+topic+"&channel="+channel, "application/json", nil)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	return body, nil
}
