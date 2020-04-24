package etcd

import (
	"fmt"
	"testing"
)

func TestPrint(t *testing.T) {
	fmt.Println("testing InitClient")
}

func TestInitClient(t *testing.T) {
	fmt.Println("testing InitClient")
	_, _ = initClient(&EtcdConfig{[]string{"http://10.72.124.102:3000"}, "root", "1"})
}

func TestPut(t *testing.T) {
	fmt.Println("testing Put")
	err := Put(&EtcdConfig{[]string{"http://10.72.124.102:3000"}, "root", "1"},
		"testkey2", "2000")
	if err != nil {
		t.Logf("put action failed")
	} else {
		t.Logf("put action succeed")
	}

}

func TestGet(t *testing.T) {
	fmt.Println("testing Get")
	value, err := Get(&EtcdConfig{[]string{"http://10.72.124.102:3000"}, "root", "1"},
		"testkey2")
	if err != nil {
		t.Logf("Get( action failed")
	} else {
		fmt.Println("testkey2: ", value.Value)
		t.Logf("get action succeed")
	}
}

func TestGetWithRev(t *testing.T) {
	fmt.Println("testing GetWithRev")
	value, err := GetWithRev(&EtcdConfig{[]string{"http://10.72.124.102:3000"}, "root", "1"},
		"testkey2", 46)
	if err != nil {
		t.Logf("GetWithRev action failed")
	} else {
		fmt.Println("testkey2: ", value.Value)
		t.Logf("get action succeed")
	}
}

func TestGetWithRange(t *testing.T) {
	fmt.Println("testing GetWithRange")
	res, err := GetWithRange(&EtcdConfig{[]string{"http://10.72.124.102:3000"}, "root", "1"},
		"testkey", "testkey3")
	if err != nil {
		t.Logf("GetWithRange action failed")
	} else {
		for _, keyinfo := range res {
			fmt.Printf("key : %s,value : %s\n", keyinfo.Key, keyinfo.Value)
		}
	}
}

func TestGetWithPrefix(t *testing.T) {
	fmt.Println("testing GetWithPrefix")
	res, err := GetWithPrefix(&EtcdConfig{[]string{"http://10.72.124.102:3000"}, "root", "1"},
		"testkey")
	if err != nil {
		t.Logf("GetWithPrefix action failed")
	} else {
		for _, keyinfo := range res {
			fmt.Printf("key : %s,value : %s\n", keyinfo.Key, keyinfo.Value)
		}
	}
}

func TestDelete(t *testing.T) {
	fmt.Println("testing delete")
	res, err := Delete(&EtcdConfig{[]string{"http://10.72.124.102:3000"}, "root", "1"},
		"key")
	if err != nil {
		t.Logf("delete action failed")
	} else {
		fmt.Println("deleted: ", res)
		t.Log("Delete action success")
	}
}

func TestDeleteWithRange(t *testing.T) {
	fmt.Println("testing DeleteWithRange")
	res, err := DeleteWithRange(&EtcdConfig{[]string{"http://10.72.124.102:3000"}, "root", "1"},
		"key", "key3")
	if err != nil {
		t.Logf("DeleteWithRange action failed")
	} else {
		fmt.Println("deleted: ", res)
		t.Log("DeleteWithRange action success")
	}
}

func TestDeleteWithPrefix(t *testing.T) {
	fmt.Println("testing DeleteWithPrefix")
	res, err := DeleteWithPrefix(&EtcdConfig{[]string{"http://10.72.124.102:3000"}, "root", "1"},
		"key")
	if err != nil {
		t.Logf("DeleteWithPrefix action failed")
	} else {
		fmt.Println("deleted: ", res)
		t.Log("DeleteWithPrefix action success")
	}
}

func TestLock(t *testing.T) {
	fmt.Println("testing Lock")
	res, err := Lock(&EtcdConfig{[]string{"http://10.72.124.102:3000"}, "root", "1"},
		[]byte("test1"), 50)
	if err != nil {
		t.Logf("Lock action failed")
	} else {
		fmt.Println("lock key: ", string(res))
		t.Log("Lock action success")
	}
}

func TestUnLock(t *testing.T) {
	fmt.Println("testing Lock")
	err := UnLock(&EtcdConfig{[]string{"http://10.72.124.102:3000"}, "root", "1"},
		[]byte("test1/694d6c88d16856f8"))
	if err != nil {
		t.Logf("Lock action failed")
	} else {
		t.Log("Lock action success")
	}
}

func TestWatch(t *testing.T) {
	fmt.Println("testing Watch")
	err := Watch(&EtcdConfig{[]string{"http://10.72.124.102:3000"}, "root", "1"},
		"key", func(info KeyInfo) {
			fmt.Println("key: ", info.Key)
			fmt.Println("value: ", info.Value)
			fmt.Println("modeRevision: ", info.ModeRevision)
		})
	if err != nil {
		t.Logf("Watch action failed")
	} else {
		t.Log("Watch action success")
	}
}

func TestWatchWithPrefix(t *testing.T) {
	fmt.Println("testing WatchWithPrefix")
	err := Watch(&EtcdConfig{[]string{"http://10.72.124.102:3000"}, "root", "1"},
		"key", func(info KeyInfo) {
			fmt.Println("key: ", info.Key)
			fmt.Println("value: ", info.Value)
			fmt.Println("modeRevision: ", info.ModeRevision)
		})
	if err != nil {
		t.Logf(" WatchWithPrefix action failed")
	} else {
		t.Log(" WatchWithPrefix action success")
	}
}
