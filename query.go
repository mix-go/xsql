package xsql

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

type query struct {
	Query
}

func (t *query) Fetch(query string, args []interface{}, opts *Options) (*Fetcher, error) {
	startTime := time.Now()
	r, err := t.Query.Query(query, args...)
	l := &Log{
		Time:         time.Now().Sub(startTime),
		SQL:          query,
		Bindings:     args,
		RowsAffected: 0,
		Error:        err,
	}
	if err != nil {
		if opts.DebugFunc != nil {
			opts.DebugFunc(l)
		}
		return nil, err
	}

	f := &Fetcher{
		R:       r,
		Log:     l,
		Options: opts,
	}
	return f, err
}

// WhereObj
// @Description: where条件语句组装器
type WhereObj struct {
	Str string
}

func WhereMaker() *WhereObj {
	return &WhereObj{
		Str: "",
	}
}
func (this *WhereObj) FieldCondi(field, exp string, value any) *WhereObj {
	t := reflect.TypeOf(value)
	if t.Name() == "int" || t.Name() == "int32" || t.Name() == "int64" || t.Name() == "int16" {
		this.Str += fmt.Sprintf("%s %s %v AND ", field, exp, value)
	} else {
		this.Str += fmt.Sprintf("%s %s '%v' AND ", field, exp, value)
	}
	return this
}

func (this *WhereObj) ToStr() string {
	this.Str = strings.TrimSuffix(this.Str, " AND ")
	s := "WHERE " + this.Str
	this.Str = ""
	return s
}
