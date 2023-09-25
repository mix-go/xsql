package xsql

import (
	"fmt"
	"strings"
)

type Condition struct {
	dataType string //Oracle Mysql Mssql Sqlite
	where    []string
	order    string
	limit    string
}

func (this Condition) Where(str string) {
	this.where = append(this.where, str)
}

func (this Condition) Limit(offset int, size int) {
	limit := ""
	switch this.dataType {
	case "Mysql":
	case "Sqlite":
		limit = fmt.Sprintf("limit %d,%d", offset, size)
		break
	}
	this.limit = limit
}

func (this Condition) Order(fields string, order string) {
	this.order = fmt.Sprintf("order by %s %s", fields, order)
}

/*
@Description: 构建条件为字符串
@receiver this
@return string
*/
func (this Condition) Build() string {
	where := "where " + strings.Join(this.where, " and ")
	var cond string
	switch this.dataType {
	case "Mysql,Sqlite":
		cond = where + " " + this.order + " " + this.limit
		break
	}
	return cond
}

func NewCondition(dataType string) Condition {
	return Condition{
		dataType: dataType,
	}
}
