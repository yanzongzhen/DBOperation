package orm

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/yanzongzhen/DBOperation/mysql"
	"github.com/yanzongzhen/Logger/logger"
	"github.com/yanzongzhen/utils"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type mapStringScan struct {
	// cp are the column pointers
	cp []interface{}
	// row contains the final result
	row map[string]string
	// col count
	colCount int
	// col names list
	colNames []string
	// col type list
	colTypes []*sql.ColumnType
}

var (
	NotEmpty = "notEmpty"            // string is not null
	Layout   = "2006-01-02 15:04:05" // datetime layout
)

func NewMapStringScan(columnNames []string) *mapStringScan {
	lenCN := len(columnNames)
	s := &mapStringScan{
		cp:       make([]interface{}, lenCN),
		row:      make(map[string]string, lenCN),
		colCount: lenCN,
		colNames: columnNames,
		colTypes: make([]*sql.ColumnType, 0),
	}
	for index := 0; index < lenCN; index++ {
		s.cp[index] = new(sql.RawBytes)
	}
	return s
}

func (s *mapStringScan) Update(rows *sql.Rows) error {
	if err := rows.Scan(s.cp...); err != nil {
		return err
	}
	t, _ := rows.ColumnTypes()
	s.colTypes = t
	for index := 0; index < s.colCount; index++ {
		if rb, ok := s.cp[index].(*sql.RawBytes); ok {
			s.row[s.colNames[index]] = string(*rb)
			*rb = nil // reset pointer to discard current value to avoid a bug
		} else {
			return errors.New("Cannot convert column " + s.colNames[index] + " to type *sql.RawBytes")
		}
	}
	return nil
}

func (s *mapStringScan) Get() map[string]string {
	return s.row
}

func (s *mapStringScan) GetByName(name string) string {
	return s.row[name]
}

func (s *mapStringScan) Unmarshal(ptr interface{}) error {
	data := s.Get()
	v := reflect.ValueOf(ptr).Elem()
	if reflect.TypeOf(ptr).Kind() != reflect.Ptr {
		return errors.New("param must be pointer")
	}
	// ptr is map
	if reflect.ValueOf(ptr).Elem().Kind() == reflect.Map {
		for index := 0; index < s.colCount; index++ {
			value := s.GetByName(s.colNames[index])
			// database column type
			tt := strings.ToLower(s.colTypes[index].DatabaseTypeName())
			// map key
			key := reflect.ValueOf(s.colNames[index])
			switch tt {
			case "integer", "tinyint", "smallint", "bigint", "mediumint", "int":
				i, err := formatInt(value)
				if err != nil {
					return err
				}
				v.SetMapIndex(key, reflect.ValueOf(i))
			case "double", "float", "decimal":
				i, err := formatFloat(value)
				if err != nil {
					return err
				}
				v.SetMapIndex(key, reflect.ValueOf(i))
			case "datetime":
				v.SetMapIndex(key, reflect.ValueOf(formatDatetime(value)))
			default:
				v.SetMapIndex(key, reflect.ValueOf(value))
			}
		}
		return nil
	}
	// not map but it is struct
	if reflect.ValueOf(ptr).Elem().Kind() != reflect.Struct {
		return errors.New("param must be struct pointer")
	}
	for i := 0; i < v.NumField(); i++ {
		// Structure field
		fieldInfo := v.Type().Field(i)
		tag := fieldInfo.Tag
		name := tag.Get("orm")
		if name == "" {
			// Structure field name is used by default
			name = strings.ToLower(fieldInfo.Name)
		}
		// If empty or not transmitted, set to default
		defaultValue := tag.Get("default")
		var value string
		if values, ok := data[name]; !ok {
			value = defaultValue
		} else {
			value = values
			if utils.IsEmpty(value) {
				value = defaultValue
			}
		}
		// valid allow append
		// Non-null verification in advance, solve the problem that Int is initialized to 0 under empty conditions
		valid := tag.Get("valid")
		if strings.Index(valid, NotEmpty) != -1 {
			if value == "" {
				return errors.New(fieldInfo.Name + " value not empty")
			}
		}
		// data binding
		if err := s.unmarshal(v.Field(i), value); err != nil {
			return fmt.Errorf("%s: %v", name, err)
		}
	}
	return nil
}

func (s *mapStringScan) unmarshal(field reflect.Value, value string) error {
	var err error
	switch field.Kind() {
	case reflect.String:
		field.SetString(value)
	case reflect.Int:
		var i int64
		i, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			err = errors.New("param type not match,require:int")
		}
		field.SetInt(i)
	case reflect.Bool:
		b := false
		b, err = strconv.ParseBool(value)
		if err != nil {
			err = errors.New("param type not match,require:bool")
		}
		field.SetBool(b)
	case reflect.Float64:
		var floatV float64
		floatV, err = strconv.ParseFloat(value, 64)
		if err != nil {
			err = errors.New("param type not match,require:float")
		}
		field.SetFloat(floatV)
	default:
		return fmt.Errorf("unsupported kind %s", field.Type())
	}
	return err
}

func formatDatetime(source string) string {
	tmp := strings.Split(source, "+")
	newValue := strings.Replace(tmp[0], "T", " ", 1)
	t, err := time.Parse(Layout, newValue)
	if err != nil {
		return source
	}
	return t.Format(Layout)
}

func formatInt(source string) (int64, error) {
	i, err := strconv.ParseInt(source, 10, 64)
	if err != nil {
		err = errors.New("param type not match,require:int")
	}
	return i, err
}

func formatFloat(source string) (float64, error) {
	i, err := strconv.ParseFloat(source, 64)
	if err != nil {
		err = errors.New("param type not match,require:float")
	}
	return i, err
}

func formatBool(source string) (bool, error) {
	i, err := strconv.ParseBool(source)
	if err != nil {
		err = errors.New("param type not match,require:bool")
	}
	return i, err
}

func Query(db *mysql.DBConfig, Sql string, ptr interface{}, args ...interface{}) error {
	if reflect.TypeOf(ptr).Kind() != reflect.Ptr {
		return errors.New("v must be pointer")
	}
	err := mysql.Query(db, Sql, func(rows *sql.Rows) error {
		columns, err := rows.Columns()
		if err != nil {
			return err
		}
		mapScan := NewMapStringScan(columns)
		for rows.Next() {
			err = mapScan.Update(rows)
			if err != nil {
				logger.Error(err)
			} else {
				result := mapScan.Get()
				logger.Debug(result)
			}
		}
		err = mapScan.Unmarshal(ptr)
		if err != nil {
			logger.Error(err)
		}
		return nil
	}, args...)
	return err
}
