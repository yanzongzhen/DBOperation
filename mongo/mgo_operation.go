package mongo

import (
	"errors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"sync"
)

var (
	ErrNotFound = errors.New("not found")
)

var mongoSession = make(map[string]*mgo.Session)
var lock sync.RWMutex

type ResultParser func(query *mgo.Query) error

type DBInfo struct {
	collectionName string
	dbName         string
	ip             string
}

func NewDBInfo(collectionName string, dbName string, ip string) *DBInfo {
	return &DBInfo{collectionName: collectionName, dbName: dbName, ip: ip}
}

func dealCollection(dbInfo *DBInfo, s func(c *mgo.Collection) error) error {

	//session, err := mgo.Dial(dbInfo.ip)
	//if err != nil {
	//	return err
	//}

	var err error
	lock.RLock()
	sess, ok := mongoSession[dbInfo.ip]
	lock.RUnlock()
	if !ok {
		lock.Lock()
		sess, ok = mongoSession[dbInfo.ip]
		if !ok {
			sess, err = mgo.Dial(dbInfo.ip)
			if err != nil {
				lock.Unlock()
				return err
			}
			mongoSession[dbInfo.ip] = sess
			lock.Unlock()
		} else {
			lock.Unlock()
		}
	}
	//defer session.Close()
	c := sess.DB(dbInfo.dbName).C(dbInfo.collectionName)
	err = s(c)
	if err != nil {
		lock.Lock()
		delete(mongoSession, dbInfo.ip)
		lock.Unlock()
	}
	return err
}

func Insert(dbInfo *DBInfo, docs ...interface{}) error {
	return dealCollection(dbInfo, func(c *mgo.Collection) error {
		return c.Insert(docs...)
	})
}

func QueryByDoc(dbInfo *DBInfo, queryDoc bson.D, parser ResultParser) error {
	return dealCollection(dbInfo, func(c *mgo.Collection) error {
		result := c.Find(queryDoc)
		return parser(result)
	})
}

func QueryByMap(dbInfo *DBInfo, queryDoc bson.M, parser ResultParser) error {
	return dealCollection(dbInfo, func(c *mgo.Collection) error {
		result := c.Find(queryDoc)
		return parser(result)
	})
}

func QueryByMapWithFilter(dbInfo *DBInfo, queryDoc []bson.M, parser ResultParser) error {
	return dealCollection(dbInfo, func(c *mgo.Collection) error {
		result := c.Find(queryDoc)
		return parser(result)
	})
}

func UpdateOneByMap(dbInfo *DBInfo, queryDoc bson.M, setResult interface{}) error {
	return dealCollection(dbInfo, func(c *mgo.Collection) error {
		err := c.Update(queryDoc, setResult)
		if err == mgo.ErrNotFound {
			return ErrNotFound
		}
		return err
	})
}

func UpdateAllByMap(dbInfo *DBInfo, queryDoc bson.M, setResult bson.M) (*mgo.ChangeInfo, error) {

	var res *mgo.ChangeInfo
	var err error

	err = dealCollection(dbInfo, func(c *mgo.Collection) error {
		res, err = c.UpdateAll(queryDoc, setResult)
		return err
	})
	return res, err
}

func DeleteByMap(dbInfo *DBInfo, queryDoc bson.M) error {
	return dealCollection(dbInfo, func(c *mgo.Collection) error {
		err := c.Remove(queryDoc)
		if err == mgo.ErrNotFound {
			return ErrNotFound
		}
		return err
	})
}

func DeleteCollection(dbInfo *DBInfo) error {
	return dealCollection(dbInfo, func(c *mgo.Collection) error {
		err := c.DropCollection()
		if err == mgo.ErrNotFound {
			return ErrNotFound
		}
		return err
	})
}

func RemoveAll(info *DBInfo) (int, error) {
	var res int
	err := dealCollection(info, func(c *mgo.Collection) error {
		i, err := c.RemoveAll(nil)
		if err == mgo.ErrNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		res = i.Removed
		return nil
	})
	return res, err
}
