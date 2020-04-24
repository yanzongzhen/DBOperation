package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/yanzongzhen/Logger/logger"
	_ "github.com/go-sql-driver/mysql"
	"net/url"
	"strconv"
	"sync"
	"time"
)

type RowsParser func(rows *sql.Rows) error
type ResultParser func(result sql.Result) error
type dbOperator func(db *sql.DB) error

var ErrorNotFound = errors.New("Not Found")

type DBConfig struct {
	UserName          string `json:"user_name"`
	PassWord          string `json:"password"`
	DBAddress         string `json:"db_address"`
	Port              int    `json:"port"`
	DBName            string `json:"db_name"`
	MaxOpenConnection int    `json:"max_open_connection"`
	MaxIdleConnection int    `json:"max_idle_connection"`
	ConnMaxLifetime   int    `json:"conn_max_lifetime"`
}

func (config *DBConfig) getDBDataSource() string {
	return config.UserName + ":" + config.PassWord + "@tcp(" +
		config.DBAddress + ":" + strconv.Itoa(config.Port) + ")/" + config.DBName + "?charset=utf8&parseTime=true&loc=" + url.QueryEscape("Asia/Shanghai")
}

func NewMySqlConfig(userName string, password string, ip string, port int, dbName string) *DBConfig {
	return NewMySqlConfigWithConnConfig(userName, password, ip, port, dbName, 10, 8, 3600)
}

func NewMySqlConfigWithConnConfig(userName string, password string, ip string, port int, dbName string, maxOpenConnection int, maxIdleConnection int, connMaxLifeTime int) *DBConfig {
	if maxOpenConnection == 0 {
		maxOpenConnection = 10
	}
	if maxIdleConnection == 0 {
		maxIdleConnection = 8
	}
	if connMaxLifeTime == 0 {
		connMaxLifeTime = 3600
	}
	return &DBConfig{UserName: userName, PassWord: password, DBAddress: ip, Port: port, DBName: dbName, MaxOpenConnection: maxOpenConnection, MaxIdleConnection: maxIdleConnection, ConnMaxLifetime: connMaxLifeTime}
}

var lock sync.RWMutex

//var db *sql.DB
var dbMap map[string]*sql.DB

func init() {
	dbMap = make(map[string]*sql.DB)
}

func dealMySql(sqlConfig *DBConfig, operator dbOperator, num int) error {

	maxOpen := sqlConfig.MaxOpenConnection
	maxIdle := sqlConfig.MaxIdleConnection
	lifeTime := time.Duration(sqlConfig.ConnMaxLifetime) * time.Second

	//var err error
	dataSourceStr := sqlConfig.getDBDataSource()
	lock.RLock()
	db, ok := dbMap[dataSourceStr]
	lock.RUnlock()

	if ok {
		//if err := db.Ping(); err == nil {
		err := operator(db)
		//} else {
		//if err != nil {
		//	logger.Debugln("invalid connection", err)
		//	lock.Lock()
		//	delete(dbMap, dataSourceStr)
		//	if db != nil {
		//		_ = db.Close()
		//	}
		//	lock.Unlock()
		//	//lock.Unlock()
		//	//newDB, err := sql.Open("mysql", dataSourceStr)
		//	//if err != nil {
		//	//	logger.Debugln(err)
		//	//	return err
		//	//}
		//	//lock.Lock()
		//	//dbMap[dataSourceStr] = newDB
		//	return err
		//}
		return err
		//}
	} else {
		lock.Lock()
		db, ok := dbMap[dataSourceStr]
		if ok {
			lock.Unlock()
			return operator(db)
		}

		logger.Debug("new ===================connection")
		newDB, err := sql.Open("mysql", dataSourceStr)
		if err != nil {
			logger.Debugln(err)
			lock.Unlock()
			return err
		}

		newDB.SetMaxOpenConns(maxOpen)
		newDB.SetMaxIdleConns(maxIdle)
		newDB.SetConnMaxLifetime(lifeTime)
		dbMap[dataSourceStr] = newDB
		lock.Unlock()
		return operator(newDB)
	}
}

func dealMySqlWithTranstion(sqlConfig *DBConfig, operator dbOperator) error {

	//var err error
	dataSourceStr := sqlConfig.getDBDataSource()

	lock.RLock()
	db, ok := dbMap[dataSourceStr]
	lock.RUnlock()

	if ok {
		if err := db.Ping(); err == nil {
			return operator(db)
		} else {
			logger.Debugln("invaild connection")
			lock.Lock()
			delete(dbMap, dataSourceStr)
			newDB, err := sql.Open("mysql", dataSourceStr)
			dbMap[dataSourceStr] = newDB
			lock.Unlock()
			if err != nil {
				return err
			}
			return operator(newDB)
		}
	} else {
		lock.Lock()
		newDB, err := sql.Open("mysql", dataSourceStr)
		dbMap[dataSourceStr] = newDB
		lock.Unlock()
		if err != nil {
			return err
		}
		return operator(newDB)
	}

}

func Query(sqlConfig *DBConfig, sqlSentence string, parser RowsParser, args ...interface{}) error {
	logger.Debugf("sqlsentence:%s args:%v", sqlSentence, args)
	return dealMySql(sqlConfig, func(db *sql.DB) error {
		rows, err := db.Query(sqlSentence, args...)
		defer func() {
			if rows != nil {
				_ = rows.Close()
			}
		}()
		if err != nil {
			logger.Debugln(err)
			return err
		}

		err = parser(rows)
		if err != nil {
			return err
		}
		return nil
	}, 1)

}

func Insert(sqlConfig *DBConfig, sqlSentence string, parser ResultParser, args ...interface{}) error {
	logger.Debugf("sqlsentence:%s args:%v\n", sqlSentence, args)
	return dealMySql(sqlConfig, func(db *sql.DB) error {
		result, err := db.Exec(sqlSentence, args...)
		if err != nil {
			return err
		}
		if parser != nil {
			return parser(result)
		}
		return nil
	}, 1)
}

func InsertByTranstion(sqlConfig *DBConfig, sqlSentence string, args [][]interface{}) error {
	return dealMySql(sqlConfig, func(db *sql.DB) error {
		conn, err := db.Begin()
		if err != nil {
			return err
		}
		for _, arg := range args {
			r, err := conn.Exec(sqlSentence, arg...)
			if err != nil {
				return err
			}
			_, err = r.LastInsertId()
			if err != nil {
				fmt.Println("exec failed,", err)
				//回滚
				_ = conn.Rollback()
				return err
			}
		}
		return conn.Commit()
	}, 1)
}

func Delete(sqlConfig *DBConfig, sqlSentence string, parser ResultParser, args ...interface{}) error {
	//logger.Debugf("sqlsentence:%s args:%v\n", sqlSentence, args)
	return Insert(sqlConfig, sqlSentence, parser, args...)
}

func Update(sqlConfig *DBConfig, sqlSentence string, parser ResultParser, args ...interface{}) error {
	return Insert(sqlConfig, sqlSentence, parser, args...)
}

func Create(sqlConfig *DBConfig, sqlSentence string) error {
	logger.Debugf("sqlsentence:%s", sqlSentence)
	return dealMySql(sqlConfig, func(db *sql.DB) error {
		st, err := db.Prepare(sqlSentence)
		if err != nil {
			return err
		}
		_, err = st.Exec()
		return err
	}, 1)
}

func ExecSql(sqlConfig *DBConfig, sqlSentence string) error {
	return Create(sqlConfig, sqlSentence)
}
