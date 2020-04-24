package zk

import (
	"errors"
	"fmt"
	"github.com/yanzongzhen/Logger/logger"
	"github.com/samuel/go-zookeeper/zk"
	"sync"
	"time"
)

var connectZKError = errors.New("connect zk failed")

type WatchEventType int

const (
	NodeCreate WatchEventType = iota
	NodeUpdate
	NodeDelete
	NodeWatchError
	NodeChindrenChange
	UnKnownType
)

type NodeWatchCallBack func(eventType WatchEventType, path string, version int32, newValue []byte)

var connectionInfo map[string]*zk.Conn
var lock sync.RWMutex
var stopChanel map[string]chan int

func init() {
	connectionInfo = make(map[string]*zk.Conn)
	stopChanel = make(map[string]chan int)
}

func dealConnection(url string, f func(conn *zk.Conn) error) error {
	lock.RLock()
	conn, ok := connectionInfo[url]
	lock.RUnlock()
	if ok {
		err := f(conn)
		if err != nil {
			return doOperation(f, conn, url)
		}
		return nil
	} else {
		lock.Lock()
		defer lock.Unlock()
		conn, ok := connectionInfo[url]
		if ok {
			return doOperation(f, conn, url)
		}

		var hosts = []string{url} //server端host
		conn, _, err := zk.Connect(hosts, time.Second*5)
		conn.SetLogger(logger.GetLogger(logger.DEBUG))
		if err != nil {
			return connectZKError
		}
		connectionInfo[url] = conn
		return doOperation(f, conn, url)
	}
}

func doOperation(f func(conn *zk.Conn) error, conn *zk.Conn, url string) error {
	err := f(conn)
	if err != nil {

		lock.Lock()
		if _, ok := connectionInfo[url]; ok {
			delete(connectionInfo, url)
			conn.Close()
		}

		var hosts = []string{url} //server端host
		conn, _, err = zk.Connect(hosts, time.Second*5)
		conn.SetLogger(logger.GetLogger(logger.DEBUG))
		if err != nil {
			lock.Unlock()
			return connectZKError
		}
		connectionInfo[url] = conn
		lock.Unlock()
		err = f(conn)
		return err
	}
	return nil
}

//func CreateDir(url string, path string) error {
//	return dealConnection(url, func(conn *zk.Conn) error {
//		//acls := zk.DigestACL(zk.PermAll, "icity_go", "sixsixsix")
//		acls := zk.WorldACL(zk.PermAll)
//		res, err := conn.Create(path, nil, 0, acls)
//		if err != nil {
//			logger.Errorf("create failed path:%s error:%v", path, err)
//			return err
//		}
//		logger.Debugf("create success %s", res)
//		return nil
//	})
//}

func Create(url string, path string, value []byte) error {
	return dealConnection(url, func(conn *zk.Conn) error {
		//acls := zk.DigestACL(zk.PermAll, "icity_go", "sixsixsix")
		acls := zk.WorldACL(zk.PermAll)
		res, err := conn.Create(path, value, 0, acls)
		if err != nil {
			logger.Errorf("create failed path:%s error:%v", path, err)
			return err
		}
		logger.Debugf("create success %s", res)
		return nil
	})
}

func Exists(url string, path string) (bool, int32, error) {
	isExist := false
	var version int32
	err := dealConnection(url, func(conn *zk.Conn) error {
		res, stat, err := conn.Exists(path)
		if err != nil {
			logger.Errorf("exists failed path:%s  error:%v", path, err)
			return err
		}
		isExist = res
		version = stat.Version
		logger.Debugf("exists success %s", zkStateString(stat))
		return nil
	})

	if err != nil {
		return isExist, -1, err
	}

	return isExist, version, nil

}

func Update(url string, path string, newValue []byte, version int32) error {
	return dealConnection(url, func(conn *zk.Conn) error {
		s, err := conn.Set(path, newValue, version)
		if err != nil {
			logger.Errorf("update failed path:%s value:%s error:%v", path, string(newValue), err)
			return err
		}
		logger.Debugf("Update success %s", zkStateString(s))
		return nil
	})
}

func Get(url string, path string) ([]byte, int32, error) {
	var value []byte
	var version int32

	err := dealConnection(url, func(conn *zk.Conn) error {
		res, s, err := conn.Get(path)

		if err != nil {
			logger.Errorf("get failed path:%s  error:%v", path, err)
			return err
		}
		value = res
		version = s.Version
		logger.Debugf("get success %s value : %s", zkStateString(s), string(res))
		return nil
	})

	if err != nil {
		return nil, -1, err
	}

	return value, version, nil
}
func GetChildren(url string, path string) ([]string, error) {
	var childNodes []string
	err := dealConnection(url, func(conn *zk.Conn) error {
		res, _, err := conn.Children(path)
		if err != nil {
			logger.Errorf("get children failed path:%s  error:%v", path, err)
			return err
		}
		childNodes = res
		return nil
	})

	if err != nil {
		return nil, err
	}
	return childNodes, nil
}

func Delete(url string, path string, version int32) error {
	return dealConnection(url, func(conn *zk.Conn) error {
		err := conn.Delete(path, version)
		if err != nil {
			logger.Errorf("delete failed path:%s version:%d error:%v", path, version, err)
			return err
		}
		logger.Errorf("delete success path:%s version:%d error:%v", path, version, err)
		return nil
	})
}

func RemoveWatch(path string) {
	logger.Debug("RemoveWatch not watch error")
	sc, ok := stopChanel[path]
	if ok {
		close(sc)
	} else {
		logger.Error("RemoveWatch not watch error")
	}
}

func AddWatch(url string, path string, callBack NodeWatchCallBack) {

	go func() {
		for {
			var value []byte
			var eventCh <-chan zk.Event
			err := dealConnection(url, func(conn *zk.Conn) error {
				res, s, ch, err := conn.GetW(path)
				if err != nil {
					logger.Errorf("watch failed path:%s  error:%v", path, err)
					return err
				}
				value = res
				eventCh = ch
				logger.Debug("watch path %s success %s", path, zkStateString(s))
				return nil
			})

			if err != nil {
				callBack(NodeWatchError, path, -1, nil)
			}
			res := watchNode(url, path, eventCh, callBack)
			if res {
				break
			}
		}
	}()

}

func WatchChildren(url string, path string, callBack NodeWatchCallBack) {

	go func() {
		for {
			var childNodes []string
			var eventCh <-chan zk.Event
			err := dealConnection(url, func(conn *zk.Conn) error {
				nodes, s, ch, err := conn.ChildrenW(path)
				if err != nil {
					logger.Errorf("watch failed %s", err.Error())
					return err
				}
				childNodes = nodes
				eventCh = ch
				logger.Debug("watch success %s", zkStateString(s))
				return nil
			})
			if err != nil {
				callBack(NodeWatchError, path, -1, nil)
			}

			res := watchNode(url, path, eventCh, callBack)
			if res {
				break
			}
		}
	}()
}

func watchNode(url string, path string, eventCh <-chan zk.Event, callBack NodeWatchCallBack) bool {
	var stop chan int
	stop, ok := stopChanel[path]

	if !ok {
		stop = make(chan int)
		stopChanel[path] = stop
	}

	select {
	case <-stop:
		logger.Debug("stop")
		return true
	default:
		break
	}
	e, ok := <-eventCh
	if ok {
		logger.Debug("watchrecive", e.Type.String())
		switch e.Type {
		case zk.EventNodeCreated:
			value, version, err := Get(url, path)
			if err != nil {
				callBack(NodeCreate, e.Path, -1, nil)
			}
			callBack(NodeCreate, e.Path, version, value)
			break
		case zk.EventNodeDataChanged:
			value, version, err := Get(url, path)
			if err != nil {
				callBack(NodeUpdate, e.Path, -1, nil)
			} else {
				callBack(NodeUpdate, e.Path, version, value)
			}
			break
		case zk.EventNodeDeleted:
			callBack(NodeDelete, e.Path, -1, nil)
			break
		case zk.EventNotWatching:
			callBack(NodeWatchError, e.Path, -1, nil)
			break
		case zk.EventNodeChildrenChanged:
			callBack(NodeChindrenChange, e.Path, -1, nil)
			break
		default:
			callBack(UnKnownType, e.Path, -1, nil)
			break
		}
	}
	return false
}

func zkStateString(s *zk.Stat) string {
	return fmt.Sprintf("Czxid:%d, Mzxid: %d, Ctime: %d, Mtime: %d, Version: %d, Cversion: %d, Aversion: %d, EphemeralOwner: %d, DataLength: %d, NumChildren: %d, Pzxid: %d",
		s.Czxid, s.Mzxid, s.Ctime, s.Mtime, s.Version, s.Cversion, s.Aversion, s.EphemeralOwner, s.DataLength, s.NumChildren, s.Pzxid)
}

func zkStateStringFormat(s *zk.Stat) string {
	return fmt.Sprintf("Czxid:%d\nMzxid: %d\nCtime: %d\nMtime: %d\nVersion: %d\nCversion: %d\nAversion: %d\nEphemeralOwner: %d\nDataLength: %d\nNumChildren: %d\nPzxid: %d\n",
		s.Czxid, s.Mzxid, s.Ctime, s.Mtime, s.Version, s.Cversion, s.Aversion, s.EphemeralOwner, s.DataLength, s.NumChildren, s.Pzxid)
}
