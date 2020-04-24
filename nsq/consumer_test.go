package nsq

import (
	"fmt"
	"testing"
	"time"
)

func TestInitNsqConsumer(t *testing.T) {
	fmt.Println("start testing InitNsqConsumer")
	err := InitNsqConsumer("test", "channel-test", false)
	if err != nil {
		fmt.Println("InitNsqConsumer failure")
		return
	}
	fmt.Println("InitNsqConsumer success")
	time.Sleep(20 * time.Second)
}
