package nsq

import (
	"errors"
	"fmt"
	"github.com/yanzongzhen/Logger/logger"
	"github.com/nsqio/go-nsq"
	"time"
)

var nsqLookupHttpAddr = "10.72.124.102:2001"

var consumer *nsq.Consumer

type consumerT struct {
}

func (*consumerT) HandleMessage(message *nsq.Message) error {
	if message != nil {
		logger.Debugln("get message from ", message.NSQDAddress, ", content is: ", string(message.Body))
		//service.StoreAndVerifyData(message.Body, true)
		return nil
	}
	return errors.New("the message is nil")
}

type consumerF struct {
}

func (*consumerF) HandleMessage(message *nsq.Message) error {
	if message != nil {
		logger.Debugln("get message from ", message.NSQDAddress, ", content is: ", string(message.Body))
		//service.StoreAndVerifyData(message.Body, false)
		return nil
	}
	return errors.New("the message is nil")
}

func InitNsqConsumer(topic string, channel string, isEmail bool) error {
	var err error
	config := nsq.NewConfig()
	config.DialTimeout = 2 * time.Second
	config.DefaultRequeueDelay = 40 * time.Second
	config.LookupdPollInterval = 2 * time.Second
	config.HeartbeatInterval = 5 * time.Second
	consumer, err = nsq.NewConsumer(topic, channel, config)
	if err != nil {
		logger.Errorf("initialize consumer failure")
		return err
	}
	if isEmail {
		consumer.AddHandler(&consumerT{})
	} else {
		consumer.AddHandler(&consumerF{})
	}
	err = consumer.ConnectToNSQLookupd(nsqLookupHttpAddr)
	if err != nil {
		fmt.Println("connect to ", nsqLookupHttpAddr, " failure!")
		return err
	}
	return nil
}
