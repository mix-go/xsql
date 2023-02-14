package xsql

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

func MakeOraSqlByStruct(data any, queryType string) (string, error) {
	table := ""
	fields := make([]string, 0)
	bindAvgs := ""
	value := reflect.ValueOf(data)
	switch value.Kind() {
	case reflect.Struct:
		if tab, ok := data.(Table); ok {
			table = tab.TableName()
		} else {
			table = value.Type().Name()
		}

		for i := 0; i < value.NumField(); i++ {
			if !value.Field(i).CanInterface() {
				continue
			}

			tag := value.Type().Field(i).Tag.Get("xsql")
			if tag == "" || tag == "-" || tag == "_" {
				continue
			}
			fields = append(fields, tag) //映射得字段名称

			switch value.Field(i).Type().String() {
			case "string":
				bindAvgs += fmt.Sprintf("'%v',", value.Field(i).Interface())
			case "time.Time": // time特殊处理
				ti := value.Field(i).Interface().(time.Time)
				t := ti.Format("2006-01-02 15:04:05")
				bindAvgs += fmt.Sprintf("TO_TIMESTAMP('%s','SYYYY-MM-DD HH24:MI:SS:FF6'),", t)
			default:
				bindAvgs += fmt.Sprintf("%v,", value.Field(i).Interface())
			}
		}
		break
	default:
		return "", errors.New("sql: only for struct type")
	}
	if queryType == "insert" {
		SQL := fmt.Sprintf(`%s %s (%s) VALUES (%s)`, "INSERT INTO", table, strings.Join(fields, ", "), strings.Trim(bindAvgs, ","))
		return SQL, nil
	}

	return "", errors.New("sql: noe")
}
