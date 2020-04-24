package hbase

import (
	"testing"
	"time"
)

func TestGet(t *testing.T) {
	config := &Config{
		Url:          "172.22.56.32,172.22.56.71,172.22.56.72",
		Table:        "hbase_test",
		ColumnFamily: "column_family_1",
		Column:       "column_1",
	}
	res, timeStamp, err := Get(config, "key_3", time.Second*3)
	if err != nil {
		t.Error(err)
	}
	t.Log(string(res), timeStamp)
}

func TestPut(t *testing.T) {
	config := &Config{
		Url:          "172.22.56.32,172.22.56.71,172.22.56.72",
		Table:        "hbase_test",
		ColumnFamily: "column_family_1",
		Column:       "column_1",
	}
	err := Put(config, "key_3", []byte("value3"), time.Second*3)
	if err != nil {
		t.Error(err)
	}
}
func TestGetTimeStamp(t *testing.T) {
	config := &Config{
		Url:          "172.22.56.32,172.22.56.71,172.22.56.72",
		Table:        "hbase_test",
		ColumnFamily: "column_family_1",
		Column:       "column_1",
	}
	res, err := GetTimeStamp(config, "key_3", time.Second*3)
	if err != nil {
		t.Error(err)
	}
	t.Log(res.Format("2006-01-02 15:04:05"))
}

func TestDelete(t *testing.T) {
	config := &Config{
		Url:          "172.22.56.32,172.22.56.71,172.22.56.72",
		Table:        "hbase_test",
		ColumnFamily: "column_family_1",
		Column:       "column_1",
	}
	t.Log(Delete(config, "key_3", time.Second*3))
}
