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

func (s *mapStringScan) GetColumnType(name string) *sql.ColumnType {
	for i, c := range s.colNames {
		if c == name {
			return s.colTypes[i]
		}
	}
	return nil
}

func indirect(v reflect.Value, decodingNull bool) reflect.Value {
	v0 := v
	haveAddr := false

	if v.Kind() != reflect.Ptr && v.Type().Name() != "" && v.CanAddr() {
		haveAddr = true
		v = v.Addr()
	}
	for {
		// Load value from interface, but only if the result will be
		// usefully addressable.
		if v.Kind() == reflect.Interface && !v.IsNil() {
			e := v.Elem()
			if e.Kind() == reflect.Ptr && !e.IsNil() && (!decodingNull || e.Elem().Kind() == reflect.Ptr) {
				haveAddr = false
				v = e
				continue
			}
		}

		if v.Kind() != reflect.Ptr {
			break
		}

		if v.Elem().Kind() != reflect.Ptr && decodingNull && v.CanSet() {
			break
		}

		// Prevent infinite loop if v is an interface pointing to its own address:
		//     var v interface{}
		//     v = &v
		if v.Elem().Kind() == reflect.Interface && v.Elem().Elem() == v {
			v = v.Elem()
			break
		}
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		if haveAddr {
			v = v0 // restore original value after round-trip Value.Addr().Elem()
			haveAddr = false
		} else {
			v = v.Elem()
		}
	}
	return v
}

func (s *mapStringScan) Unmarshal(value reflect.Value) error {

	data := s.Get()
	v := indirect(value, false)
	t := v.Type()

	switch v.Kind() {
	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			return errors.New("key type not support:" + t.Key().Kind().String())
		}
		if v.IsNil() {
			v.Set(reflect.MakeMap(t))
		}
		break
	case reflect.Struct:
		break
	default:
		return errors.New("un support type:" + v.Kind().String())
	}

	// ptr is map
	if v.Kind() == reflect.Map {
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

	for i := 0; i < v.NumField(); i++ {
		// Structure field
		fieldInfo := v.Type().Field(i)
		tag := fieldInfo.Tag
		name := tag.Get("orm")
		if name == "-" {
			continue
		}
		if utils.IsEmpty(name) {
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
		cType := s.GetColumnType(name)
		if cType == nil {
			continue
		}
		// data binding
		if err := s.unmarshal(v.Field(i), value, cType); err != nil {
			//return fmt.Errorf("%s: %v", name, err)
			logger.Error(err)
			continue
		}
	}
	return nil
}

func (s *mapStringScan) unmarshal(field reflect.Value, value string, cType *sql.ColumnType) error {
	//logger.Debug(cType.Name())
	//logger.Debug(cType.DatabaseTypeName())
	if field.Kind() == reflect.String {
		field.SetString(value)
		return nil
	}
	parseError := errors.New(fmt.Sprintf("can't parse %s to %s", value, field.Type()))
	switch field.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		n, err := strconv.ParseUint(value, 10, 64)
		if err != nil || field.OverflowUint(n) {
			return parseError
		}
		field.SetUint(n)
		break
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || field.OverflowInt(n) {
			return parseError
		}
		field.SetInt(n)
		break
	case reflect.Bool:
		field.SetBool(value == "1" || value == "true")
		break
	case reflect.Float32, reflect.Float64:
		n, err := strconv.ParseFloat(value, field.Type().Bits())
		if err != nil || field.OverflowFloat(n) {
			return parseError
		}
		field.SetFloat(n)
		break
	case reflect.Struct:
		logger.Debug(strings.ToLower(cType.DatabaseTypeName()))
		if strings.ToLower(cType.DatabaseTypeName()) == "datetime" {
			t, err := time.Parse(time.RFC3339, value)
			if err != nil {
				logger.Error(err)
				return parseError
			}
			if field.Type() == reflect.TypeOf(t) {
				field.Set(reflect.ValueOf(t))
			}
		} else {
			return parseError
		}
		break
	default:
		return parseError
	}
	return nil
	//var err error
	//switch field.Kind() {
	//case reflect.String:
	//	field.SetString(value)
	//	break
	//case reflect.Int:
	//	var i int64
	//	i, err = strconv.ParseInt(value, 10, 64)
	//	if err != nil {
	//		err = errors.New("param type not match,require:int")
	//	}
	//	field.SetInt(i)
	//	break
	//case reflect.Bool:
	//	b := false
	//	b, err = strconv.ParseBool(value)
	//	if err != nil {
	//		err = errors.New("param type not match,require:bool")
	//	}
	//	field.SetBool(b)
	//	break
	//case reflect.Float64:
	//	var floatV float64
	//	floatV, err = strconv.ParseFloat(value, 64)
	//	if err != nil {
	//		err = errors.New("param type not match,require:float")
	//	}
	//	field.SetFloat(floatV)
	//	break
	//case reflect.Struct:
	//	if cType.DatabaseTypeName() == "datatime"
	//	break
	//default:
	//	return fmt.Errorf("unsupported kind %s", field.Kind())
	//}
	//return err
}

func formatDatetime(source string) string {
	//tmp := strings.Split(source, "+")
	//newValue := strings.Replace(tmp[0], "T", " ", 1)
	t, err := time.Parse(time.RFC3339, source)
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
	//if reflect.TypeOf(ptr).Kind() != reflect.Ptr {
	//	return errors.New("v must be pointer")
	//}
	rv := reflect.ValueOf(ptr)
	if rv.Kind() != reflect.Ptr {
		return errors.New("v must be pointer")
	}

	pv := rv.Elem()

	err := mysql.Query(db, Sql, func(rows *sql.Rows) error {
		columns, err := rows.Columns()
		if err != nil {
			return err
		}
		mapScan := NewMapStringScan(columns)
		isEmpty := true

		i := 0
		for rows.Next() {
			isEmpty = false
			err = mapScan.Update(rows)
			if err != nil {
				logger.Error(err)
			} else {
				result := mapScan.Get()
				logger.Debug(result)
			}
			if pv.Kind() == reflect.Map || pv.Kind() == reflect.Struct {
				err = mapScan.Unmarshal(rv)
				if err != nil {
					logger.Error(err)
				}
				break
			} else if pv.Kind() == reflect.Slice {
				//sliceItemType :=  pv.Elem().Kind()
				if i >= pv.Cap() {
					newcap := pv.Cap() + pv.Cap()/2
					if newcap < 4 {
						newcap = 4
					}
					newv := reflect.MakeSlice(pv.Type(), pv.Len(), newcap)
					reflect.Copy(newv, pv)
					pv.Set(newv)
				}
				if i >= pv.Len() {
					pv.SetLen(i + 1)
				}

				err = mapScan.Unmarshal(pv.Index(i))
				if err != nil {
					logger.Error(err)
					return err
				}
				i++
			} else {
				return errors.New("un support ptr type %s" + rv.Elem().Kind().String())
			}
		}
		if isEmpty {
			return mysql.ErrorNotFound
		}
		return nil
	}, args...)
	return err
}
