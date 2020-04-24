package etcd

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	//"go.etcd.io/etcd/clientv3"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3lock"
	"github.com/coreos/etcd/etcdserver/api/v3lock/v3lockpb"
	"golang.org/x/net/context"
	"sort"
	"sync"
	"time"
)

var lock sync.RWMutex

var clients map[string]*clientv3.Client

type EtcdConfig struct {
	Urls     []string
	Username string
	Password string
}

type KeyInfo struct {
	Key          string
	Value        string
	ModeRevision int
}

func generateKey(strs []string) string {
	key := ""
	sort.Sort(sort.StringSlice(strs))
	for _, str := range strs {
		key += str
	}
	h := md5.New()
	h.Write([]byte(key))
	return hex.EncodeToString(h.Sum(nil))
}

func initClient(config *EtcdConfig) (*clientv3.Client, error) {
	lock.Lock()
	defer lock.Unlock()
	var flag = true
	endPoints := config.Urls
	if len(config.Username) > 0 {
		endPoints = append(endPoints, config.Username)
		endPoints = append(endPoints, config.Username)
	}
	key := generateKey(endPoints)
	if clients == nil {
		clients = make(map[string]*clientv3.Client)
	}
	if _, ok := clients[key]; ok {
		flag = false
	}
	if flag {
		var client *clientv3.Client
		var err error
		if len(config.Username) > 0 {
			client, err = clientv3.New(clientv3.Config{
				Endpoints:   endPoints,
				DialTimeout: 5 * time.Second,
				Username:    config.Username,
				Password:    config.Password,
			})
		} else {
			client, err = clientv3.New(clientv3.Config{
				Endpoints:   endPoints,
				DialTimeout: 5 * time.Second,
			})
		}
		if err != nil {
			fmt.Println("connect failed, err:", err)
			return nil, err
		}
		clients[key] = client
		return client, nil
	} else {
		fmt.Println("the client has been existent")
		return clients[key], nil
	}
}

func getClient(config *EtcdConfig) (string, *clientv3.Client, error) {
	endpoint := config.Urls
	if len(config.Username) > 0 {
		endpoint = append(endpoint, config.Username)
		endpoint = append(endpoint, config.Username)
	}
	index := generateKey(endpoint)
	ok := false
	var client *clientv3.Client
	lock.RLock()
	client, ok = clients[index]
	lock.RUnlock()
	if !ok {
		client, err := initClient(config)
		if err != nil {
			return "", nil, err
		}
		return index, client, nil
	}
	return index, client, nil
}

func Put(config *EtcdConfig, key, value string) error {
	index, client, err := getClient(config)
	if err != nil {
		fmt.Println("put action failed")
		return err
	}
	KV := client.KV
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	_, err = KV.Put(ctx, key, value)
	if err != nil {
		_ = closeClient(index)
		return err
	}
	fmt.Println("put action succeed")
	return nil
}

func Get(config *EtcdConfig, key string) (KeyInfo, error) {
	index, client, err := getClient(config)
	if err != nil {
		fmt.Println("put action failed")
		return KeyInfo{}, err
	}
	KV := client.KV
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	r, err := KV.Get(ctx, key)
	if err != nil {
		_ = closeClient(index)
		return KeyInfo{}, err
	}
	fmt.Println("get action succeed")
	Key := string(r.Kvs[0].Key)
	Value := string(r.Kvs[0].Value)
	ModeRevision := int(r.Kvs[0].ModRevision)
	return KeyInfo{Key, Value, ModeRevision}, nil
}

// get key from service specified with version
func GetWithRev(config *EtcdConfig, key string, version int) (KeyInfo, error) {
	index, client, err := getClient(config)
	if err != nil {
		fmt.Println("put action failed")
		return KeyInfo{}, err
	}
	KV := client.KV
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	r, err := KV.Get(ctx, key, clientv3.WithRev(int64(version)))
	if err != nil {
		_ = closeClient(index)
		return KeyInfo{}, err
	}
	fmt.Println("get action succeed")
	Key := string(r.Kvs[0].Key)
	Value := string(r.Kvs[0].Value)
	ModeRevision := int(r.Kvs[0].ModRevision)
	return KeyInfo{Key, Value, ModeRevision}, nil
}

// get key from service specified with range
func GetWithRange(config *EtcdConfig, key string, endKey string) ([]KeyInfo, error) {
	index, client, err := getClient(config)
	if err != nil {
		fmt.Println("put action failed")
		return nil, err
	}
	KV := client.KV
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	r, err := KV.Get(ctx, key, clientv3.WithRange(endKey))
	if err != nil {
		_ = closeClient(index)
		return nil, err
	}
	fmt.Println("get action succeed")
	var res []KeyInfo
	for _, kv := range r.Kvs {
		Key := string(kv.Key)
		Value := string(kv.Value)
		ModeRevision := int(kv.ModRevision)
		res = append(res, KeyInfo{Key, Value, ModeRevision})
	}
	return res, nil
}

// get key from service specified with prefix
func GetWithPrefix(config *EtcdConfig, key string) ([]KeyInfo, error) {
	index, client, err := getClient(config)
	if err != nil {
		fmt.Println("put action failed")
		return nil, err
	}
	KV := client.KV
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	r, err := KV.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		_ = closeClient(index)
		return nil, err
	}
	fmt.Println("get action succeed")
	var res []KeyInfo
	for _, kv := range r.Kvs {
		Key := string(kv.Key)
		Value := string(kv.Value)
		ModeRevision := int(kv.ModRevision)
		res = append(res, KeyInfo{Key, Value, ModeRevision})
	}
	return res, nil
}

func Delete(config *EtcdConfig, key string) (int, error) {
	index, client, err := getClient(config)
	if err != nil {
		fmt.Println("put action failed")
		return 0, err
	}
	KV := client.KV
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	r, err := KV.Delete(ctx, key)
	if err != nil {
		_ = closeClient(index)
		return 0, err
	}
	fmt.Println("delete action succeed")
	res := int(r.Deleted)
	return res, nil
}

// delete keys with range
func DeleteWithRange(config *EtcdConfig, key string, endKey string) (int, error) {
	index, client, err := getClient(config)
	if err != nil {
		fmt.Println("put action failed")
		return 0, err
	}
	KV := client.KV
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	r, err := KV.Delete(ctx, key, clientv3.WithRange(endKey))
	if err != nil {
		_ = closeClient(index)
		return 0, err
	}
	fmt.Println("delete action succeed")
	res := int(r.Deleted)
	return res, nil
}

// delete key with prefix
func DeleteWithPrefix(config *EtcdConfig, key string) (int, error) {
	index, client, err := getClient(config)
	if err != nil {
		fmt.Println("put action failed")
		return 0, err
	}
	KV := client.KV
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	r, err := KV.Delete(ctx, key, clientv3.WithPrefix())
	if err != nil {
		_ = closeClient(index)
		return 0, err
	}
	fmt.Println("delete action succeed")
	res := int(r.Deleted)
	return res, nil
}

type callBack func(keyinfo KeyInfo)

func Watch(config *EtcdConfig, key string, fuc callBack) error {
	_, client, err := getClient(config)
	if err != nil {
		fmt.Println("put action failed")
		return err
	}
	ctx := context.Background()
	print(client)
	// <-chan WatchResponse
	go func() {
		fmt.Println(client)
	Loop:
		for {
			select {
			case wEvent := <-client.Watch(ctx, key):
				for _, e := range wEvent.Events {
					key := string(e.Kv.Key)
					value := string(e.Kv.Value)
					modeRevision := int(e.Kv.ModRevision)
					fuc(KeyInfo{key, value, modeRevision})
				}
				break
			case <-ctx.Done():
				break Loop
			}
		}
	}()

	return nil
}

// Watch keys with the specified prefix
func WatchWithPrefix(config *EtcdConfig, key string, fuc callBack) error {
	_, client, err := getClient(config)
	if err != nil {
		fmt.Println("put action failed")
		return err
	}
	ctx := context.Background()
	print(client)
	// <-chan WatchResponse
	go func() {
		fmt.Println(client)
	Loop:
		for {
			select {
			case wEvent := <-client.Watch(ctx, key, clientv3.WithPrefix()):
				for _, e := range wEvent.Events {
					key := string(e.Kv.Key)
					value := string(e.Kv.Value)
					modeRevision := int(e.Kv.ModRevision)
					fuc(KeyInfo{key, value, modeRevision})
				}
				break
			case <-ctx.Done():
				break Loop
			}
		}
	}()
	return nil
}

func Lock(config *EtcdConfig, name []byte, ttl int) ([]byte, error) {
	index, client, err := getClient(config)
	if err != nil {
		fmt.Println("put action failed")
		return nil, err
	}
	lockService := v3lock.NewLockServer(client)
	ctx := context.Background()
	lease := client.Lease
	leaseResponse, err := lease.Grant(ctx, int64(ttl))
	if err != nil {
		_ = closeClient(index)
		fmt.Println("can't grant a lease!")
		return nil, err
	}
	leaseID := leaseResponse.ID
	var req = &v3lockpb.LockRequest{Name: name, Lease: int64(leaseID)}
	lockResponse, err := lockService.Lock(ctx, req)
	if err != nil {
		_ = closeClient(index)
		fmt.Println("can't acquire a lock")
		return nil, err
	}
	responseKey := lockResponse.GetKey()
	return responseKey, nil
}

func UnLock(config *EtcdConfig, key []byte) error {
	index, client, err := getClient(config)
	if err != nil {
		fmt.Println("put action failed")
		return err
	}
	lockService := v3lock.NewLockServer(client)
	ctx := context.Background()
	unlockRequest := &v3lockpb.UnlockRequest{Key: key}
	_, err = lockService.Unlock(ctx, unlockRequest)
	if err != nil {
		_ = closeClient(index)
		fmt.Printf("can't unlock %v\n", key)
		return err
	}
	return nil
}

func closeClient(index string) error {
	if _, ok := clients[index]; !ok {
		fmt.Println("the client to be closed does not exist ")
		return errors.New("client does not exist")
	}
	client := clients[index]
	err := client.Close()
	if err != nil {
		fmt.Println("client close failed")
		return err
	}
	delete(clients, index)
	fmt.Println("client close successfully")
	return nil
}
