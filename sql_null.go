package xsql

import (
	"database/sql"
	"reflect"
)

type SqlNull struct {
	reflect.Value
}

func (this SqlNull) FieldTypeBasic(i int) string {
	nullStr := this.Field(i).Type().String()
	switch nullStr {
	case "sql.NullString":
		return "string"
	case "sql.NullInt64":
		return "int64"
	}
	return this.Field(i).Type().String()
}

func (this SqlNull) FieldAnyBasic(i int) any {
	nullStr := this.Field(i).Type().String()
	var v any
	switch nullStr {
	case "sql.NullString":
		v, _ = this.Field(i).Interface().(sql.NullString).Value()
		if v == nil {
			v = ""
		}
		break
	case "sql.NullInt64":
		v, _ = this.Field(i).Interface().(sql.NullInt64).Value()
		if v == nil {
			v = 0
		}
		break
	default:
		v = this.Field(i).Interface()
	}
	return v
}
