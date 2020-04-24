package nsq

import (
	"fmt"
	"testing"
)

func TestCreateTopic(t *testing.T) {
	fmt.Println("starting test CreateTopic")
	body, err := CreateTopic("10.72.124.102:3000", "http_test")
	if err != nil {
		fmt.Println("CreateTopic error")
		t.Log(err)
		return
	}
	fmt.Println("CreateTopic success")
	t.Log(string(body))
}

func TestDeleteTopic(t *testing.T) {
	fmt.Println("starting test DeleteTopic")
	body, err := DeleteTopic("10.72.124.102:3000", "http_test")
	if err != nil {
		fmt.Println("DeleteTopic error")
		t.Log(err)
		return
	}
	fmt.Println("DeleteTopic success")
	t.Log(string(body))
}

func TestEmptyTopic(t *testing.T) {
	fmt.Println("starting test EmptyTopic")
	body, err := EmptyTopic("10.72.124.102:3000", "test")
	if err != nil {
		fmt.Println("EmptyTopic error")
		t.Log(err)
		return
	}
	fmt.Println("EmptyTopic success")
	t.Log(string(body))
}

func TestPauseTopic(t *testing.T) {
	fmt.Println("starting test PauseTopic")
	body, err := PauseTopic("10.72.124.102:3000", "test")
	if err != nil {
		fmt.Println("PauseTopic error")
		t.Log(err)
		return
	}
	fmt.Println("PauseTopic success")
	t.Log(string(body))
}

func TestUnpauseTopic(t *testing.T) {
	fmt.Println("starting test UnpauseTopic")
	body, err := UnpauseTopic("10.72.124.102:3000", "test")
	if err != nil {
		fmt.Println("UnpauseTopic error")
		t.Log(err)
		return
	}
	fmt.Println("UnpauseTopic success")
	t.Log(string(body))
}

func TestCreateChannel(t *testing.T) {
	fmt.Println("starting test CreateChannel")
	body, err := CreateChannel("10.72.124.102:3000", "test", "test")
	if err != nil {
		fmt.Println("CreateChannel error")
		t.Log(err)
		return
	}
	fmt.Println("CreateChannel success")
	t.Log(string(body))
}

func TestDeleteChannel(t *testing.T) {
	fmt.Println("starting test DeleteChannel")
	body, err := DeleteChannel("10.72.124.102:3000", "test", "test")
	if err != nil {
		fmt.Println("DeleteChannel error")
		t.Log(err)
		return
	}
	fmt.Println("DeleteChannel success")
	t.Log(string(body))
}

func TestEmptyChannel(t *testing.T) {
	fmt.Println("starting test EmptyChannel")
	body, err := EmptyChannel("10.72.124.102:3000", "test", "channel-test")
	if err != nil {
		fmt.Println("EmptyChannel error")
		t.Log(err)
		return
	}
	fmt.Println("EmptyChannel success")
	t.Log(string(body))
}

func TestPauseChannel(t *testing.T) {
	fmt.Println("starting test PauseChannel")
	body, err := PauseChannel("10.72.124.102:3000", "test", "channel-test")
	if err != nil {
		fmt.Println("PauseChannel error")
		t.Log(err)
		return
	}
	fmt.Println("PauseChannel success")
	t.Log(string(body))
}

func TestUnpauseChannel(t *testing.T) {
	fmt.Println("starting test UnpauseChannel")
	body, err := UnpauseChannel("10.72.124.102:3000", "test", "channel-test")
	if err != nil {
		fmt.Println("UnpauseChannel error")
		t.Log(err)
		return
	}
	fmt.Println("UnpauseChannel success")
	t.Log(string(body))
}
