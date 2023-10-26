package xsql

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type Table interface {
	TableName() string
	DBType() string
}
type TableAttribute interface {
	Table
	PrimaryName() string //获取主键
}

type executor struct {
	Executor
}

func (t *executor) Insert(data interface{}, opts *Options) (sql.Result, error) {
	insertKey := "INSERT INTO"
	if opts.InsertKey != "" {
		insertKey = opts.InsertKey
	}
	placeholder := "?"
	if opts.Placeholder != "" {
		placeholder = opts.Placeholder
	}
	timeLayout := DefaultTimeLayout
	if opts.TimeLayout != "" {
		timeLayout = opts.TimeLayout
	}
	timeFunc := DefaultTimeFunc
	if opts.TimeFunc != nil {
		timeFunc = opts.TimeFunc
	}
	columnQuotes := "`"
	if opts.ColumnQuotes != "" {
		columnQuotes = opts.ColumnQuotes
	}
	var debugFunc DebugFunc
	if opts.DebugFunc != nil {
		debugFunc = opts.DebugFunc
	}

	fields := make([]string, 0)
	vars := make([]string, 0)
	bindArgs := make([]interface{}, 0)
	var bindArgsPrint string //打印sql插入值得字符串

	table := ""

	value := SqlNull{
		reflect.ValueOf(data),
	}
	switch value.Kind() {
	case reflect.Ptr:
		return t.Insert(value.Elem().Interface(), opts)
	case reflect.Struct:
		if tab, ok := data.(Table); ok {
			table = tab.TableName()
		} else {
			table = value.Type().Name()
		}
		param := 0
		for i := 0; i < value.NumField(); i++ {
			if !value.Field(i).CanInterface() {
				continue
			}
			fieldTypeStr := value.FieldTypeBasic(i)
			//fieldTypeStr := value.Field(i).Type().String()

			isTime := value.Field(i).Type().String() == "time.Time"

			tag := value.Type().Field(i).Tag.Get("xsql")
			if tag == "" || tag == "-" || tag == "_" {
				continue
			}
			strs := strings.Split(tag, ",")
			//是否空值忽略该字段
			var omitempy bool
			if len(strs) > 1 {
				for _, s := range strs[1:] {
					if strings.Contains(s, "omitempty") {
						omitempy = true
					}
				}
			}

			valueFieldVal := ""
			valBasic := value.FieldAnyBasic(i)
			if fieldTypeStr == "string" {
				valueFieldVal = fmt.Sprintf("%s", valBasic)
			} else if fieldTypeStr == "int" || fieldTypeStr == "int64" || fieldTypeStr == "int32" {
				valueFieldVal = fmt.Sprintf("%d", valBasic)
			} else {
				valueFieldVal = "OTHER_DATA"
			}

			//fmt.Println(value.Field(i).Type().String(),value.Field(i).Interface(),valueFieldVal)
			if omitempy && (valueFieldVal == "" || valueFieldVal == "0") {
				continue
			} else {
				fields = append(fields, strs[0])
				v := ""
				param++
				if placeholder == "?" {
					v = placeholder
				} else if placeholder == "@" {
					v = fmt.Sprintf("@p%d", param)
				} else {
					v = fmt.Sprintf(placeholder, i)
				}
				if isTime {
					vars = append(vars, timeFunc(v))
				} else {
					vars = append(vars, v)
				}

				if isTime {
					ti := value.Field(i).Interface().(time.Time)
					insertRealVal := ti.Format(timeLayout)
					bindArgsPrint += fmt.Sprintf("%s, ", insertRealVal)
					bindArgs = append(bindArgs, insertRealVal)
				} else {
					insertRealVal := value.Field(i).Interface()
					if fieldTypeStr == "string" {
						bindArgsPrint += fmt.Sprintf("'%v', ", insertRealVal)
					} else if fieldTypeStr == "[]uint8" {
						blobInsertRealVal := insertRealVal.([]uint8)
						if len(blobInsertRealVal) > 0 {
							bindArgsPrint += fmt.Sprintf("'%v', ", string(blobInsertRealVal))
						} else {
							bindArgsPrint += "'', "
							if dataTable, _ := data.(Table); dataTable.DBType() == "Oracle" {
								insertRealVal = any("")
							}
						}
					} else {
						bindArgsPrint += fmt.Sprintf("%v, ", insertRealVal)
					}

					bindArgs = append(bindArgs, insertRealVal)
				}
			}

		}
		break
	default:
		return nil, errors.New("sql: only for struct type")
	}

	SQL := fmt.Sprintf(`%s %s (%s) VALUES (%s)`, insertKey, table, columnQuotes+strings.Join(fields, columnQuotes+", "+columnQuotes)+columnQuotes, strings.Join(vars, `, `))
	bindArgsPrint = strings.TrimSuffix(bindArgsPrint, ", ")
	SQLPrint := fmt.Sprintf(`%s %s (%s) VALUES (%s)`, insertKey, table, columnQuotes+strings.Join(fields, columnQuotes+", "+columnQuotes)+columnQuotes, bindArgsPrint)
	startTime := time.Now()
	res, err := t.Executor.Exec(SQL, bindArgs...)
	var rowsAffected int64
	if res != nil {
		rowsAffected, _ = res.RowsAffected()
	}
	l := &Log{
		Time:         time.Now().Sub(startTime),
		SQL:          SQL,
		SQLPrint:     SQLPrint,
		Bindings:     bindArgs,
		RowsAffected: rowsAffected,
		Error:        err,
	}
	if debugFunc != nil {
		debugFunc(l)
	}
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (t *executor) InsertTakeLastId(data interface{}, withSeq string, query query, opts *Options) (sql.Result, error) {
	insertKey := "INSERT INTO"
	if opts.InsertKey != "" {
		insertKey = opts.InsertKey
	}
	placeholder := "?"
	if opts.Placeholder != "" {
		placeholder = opts.Placeholder
	}
	timeLayout := DefaultTimeLayout
	if opts.TimeLayout != "" {
		timeLayout = opts.TimeLayout
	}
	timeFunc := DefaultTimeFunc
	if opts.TimeFunc != nil {
		timeFunc = opts.TimeFunc
	}
	columnQuotes := "`"
	if opts.ColumnQuotes != "" {
		columnQuotes = opts.ColumnQuotes
	}
	var debugFunc DebugFunc
	if opts.DebugFunc != nil {
		debugFunc = opts.DebugFunc
	}

	fields := make([]string, 0)
	vars := make([]string, 0)
	bindArgs := make([]interface{}, 0)

	table := ""

	value := reflect.ValueOf(data)
	switch value.Kind() {
	case reflect.Ptr:
		return t.Insert(value.Elem().Interface(), opts)
	case reflect.Struct:

		if tab, ok := data.(Table); ok {
			table = tab.TableName()
		} else {
			table = value.Type().Name()
		}
		paramNum := 0
		for i := 0; i < value.NumField(); i++ {
			if !value.Field(i).CanInterface() {
				continue
			}
			fieldTypeStr := value.Field(i).Type().String()

			isTime := value.Field(i).Type().String() == "time.Time"

			tag := value.Type().Field(i).Tag.Get("xsql")
			if tag == "" || tag == "-" || tag == "_" {
				continue
			}
			strs := strings.Split(tag, ",")
			//是否空值忽略该字段
			var omitempy bool
			if len(strs) > 1 {
				for _, s := range strs[1:] {
					if strings.Contains(s, "omitempty") {
						omitempy = true
					}
				}
			}

			valueFieldVal := ""
			if fieldTypeStr == "string" {
				valueFieldVal = fmt.Sprintf("%s", value.Field(i).Interface())
			} else if fieldTypeStr == "int" || fieldTypeStr == "int64" || fieldTypeStr == "int32" {
				valueFieldVal = fmt.Sprintf("%d", value.Field(i).Interface())
			}

			//fmt.Println(value.Field(i).Type().String(),value.Field(i).Interface(),valueFieldVal)
			if omitempy && (valueFieldVal == "" || valueFieldVal == "0") {
				continue
			} else {
				fields = append(fields, strs[0])
				v := ""
				paramNum++
				if placeholder == "?" {
					v = placeholder
				} else if placeholder == "@" {
					v = fmt.Sprintf("@p%d", paramNum)
				} else {
					v = fmt.Sprintf(placeholder, i)
				}
				if isTime {
					vars = append(vars, timeFunc(v))
				} else {
					vars = append(vars, v)
				}

				if isTime {
					ti := value.Field(i).Interface().(time.Time)
					bindArgs = append(bindArgs, ti.Format(timeLayout))
				} else {
					bindArgs = append(bindArgs, value.Field(i).Interface())
				}
			}

		}
		break
	default:
		return nil, errors.New("sql: only for struct type")
	}

	SQL := fmt.Sprintf(`%s %s (%s) VALUES (%s)`, insertKey, table, columnQuotes+strings.Join(fields, columnQuotes+", "+columnQuotes)+columnQuotes, strings.Join(vars, `, `))
	startTime := time.Now()
	var res QueryRes
	var err error
	var rowsAffected int64
	dataTable, _ := data.(Table)
	switch dataTable.DBType() {
	case "Mssql":
		SQL += ";Select ISNULL(SCOPE_IDENTITY(),0) INSERT_ID"
		f, err := query.Fetch(SQL, bindArgs, opts)
		if err != nil {
			return nil, err
		}
		defer f.R.Close()
		// Fetch rows
		var lastId int64

		for f.R.Next() {
			if err = f.R.Scan(&lastId); err != nil {
				return nil, err
			}
		}
		//获取insert执行的时候是否错误
		if err := f.R.Err(); err != nil {
			return nil, err
		}
		res = QueryRes{
			InsertId: lastId,
			Affected: 1,
		}
		break
	case "Oracle":
		_, err = t.Executor.Exec(SQL, bindArgs...)
		if err != nil {
			return res, err
		}
		res = QueryRes{
			InsertId: 0,
			Affected: 1,
		}
		break
	}

	l := &Log{
		Time:         time.Now().Sub(startTime),
		SQL:          SQL,
		Bindings:     bindArgs,
		RowsAffected: rowsAffected,
		Error:        err,
	}
	if debugFunc != nil {
		debugFunc(l)
	}
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (t *executor) BatchInsert(array interface{}, opts *Options) (sql.Result, error) {
	insertKey := "INSERT INTO"
	if opts.InsertKey != "" {
		insertKey = opts.InsertKey
	}
	placeholder := "?"
	if opts.Placeholder != "" {
		placeholder = opts.Placeholder
	}
	timeLayout := DefaultTimeLayout
	if opts.TimeLayout != "" {
		timeLayout = opts.TimeLayout
	}
	columnQuotes := "`"
	if opts.ColumnQuotes != "" {
		columnQuotes = opts.ColumnQuotes
	}
	var debugFunc DebugFunc
	if opts.DebugFunc != nil {
		debugFunc = opts.DebugFunc
	}

	fields := make([]string, 0)
	valueSql := make([]string, 0)
	bindArgs := make([]interface{}, 0)

	table := ""

	// check
	value := reflect.ValueOf(array)
	switch value.Kind() {
	case reflect.Ptr:
		return t.BatchInsert(value.Elem().Interface(), opts)
	case reflect.Array, reflect.Slice:
		break
	default:
		return nil, errors.New("sql: only for struct array/slice type")
	}
	if value.Len() == 0 {
		return nil, errors.New("sql: array/slice length cannot be 0")
	}

	// fields
	switch value.Index(0).Kind() {
	case reflect.Struct:
		subValue := value.Index(0)

		if tab, ok := subValue.Interface().(Table); ok {
			table = tab.TableName()
		} else {
			table = subValue.Type().Name()
		}

		for i := 0; i < subValue.NumField(); i++ {
			if !subValue.Field(i).CanInterface() {
				continue
			}
			tag := subValue.Type().Field(i).Tag.Get("xsql")
			if tag == "" || tag == "-" || tag == "_" {
				continue
			}
			fields = append(fields, tag)
		}
		break
	default:
		return nil, errors.New("sql: only for struct array/slice type")
	}

	// values
	switch value.Kind() {
	case reflect.Slice, reflect.Array:
		ai := 0
		for r := 0; r < value.Len(); r++ {
			switch value.Index(r).Kind() {
			case reflect.Struct:
				subValue := value.Index(r)
				vars := make([]string, 0)
				for c := 0; c < subValue.NumField(); c++ {
					if !subValue.Field(c).CanInterface() {
						continue
					}

					tag := subValue.Type().Field(c).Tag.Get("xsql")
					if tag == "" || tag == "-" || tag == "_" {
						continue
					}

					if placeholder == "?" {
						vars = append(vars, placeholder)
					} else {
						vars = append(vars, fmt.Sprintf(placeholder, ai))
						ai += 1
					}

					// time特殊处理
					if subValue.Field(c).Type().String() == "time.Time" {
						ti := subValue.Field(c).Interface().(time.Time)
						bindArgs = append(bindArgs, ti.Format(timeLayout))
					} else {
						bindArgs = append(bindArgs, subValue.Field(c).Interface())
					}
				}
				valueSql = append(valueSql, fmt.Sprintf("(%s)", strings.Join(vars, `, `)))
				break
			default:
				return nil, errors.New("sql: only for struct array/slice type")
			}
		}
		break
	default:
		return nil, errors.New("sql: only for struct array/slice type")
	}

	SQL := fmt.Sprintf(`%s %s (%s) VALUES %s`, insertKey, table, columnQuotes+strings.Join(fields, columnQuotes+", "+columnQuotes)+columnQuotes, strings.Join(valueSql, ", "))

	startTime := time.Now()
	res, err := t.Executor.Exec(SQL, bindArgs...)
	var rowsAffected int64
	if res != nil {
		rowsAffected, _ = res.RowsAffected()
	}
	l := &Log{
		Time:         time.Now().Sub(startTime),
		SQL:          SQL,
		Bindings:     bindArgs,
		RowsAffected: rowsAffected,
		Error:        err,
	}
	if debugFunc != nil {
		debugFunc(l)
	}
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (t *executor) UpdateForce(data interface{}, expr string, fields []string, opts *Options) (sql.Result, error) {
	placeholder := "?"
	if opts.Placeholder != "" {
		placeholder = opts.Placeholder
	}
	timeLayout := DefaultTimeLayout
	if opts.TimeLayout != "" {
		timeLayout = opts.TimeLayout
	}
	columnQuotes := "`"
	if opts.ColumnQuotes != "" {
		columnQuotes = opts.ColumnQuotes
	}
	var debugFunc DebugFunc
	if opts.DebugFunc != nil {
		debugFunc = opts.DebugFunc
	}

	set := make([]string, 0)
	bindArgs := make([]interface{}, 0)

	table := ""

	typeVal := reflect.TypeOf(data)
	value := reflect.ValueOf(data)
	switch value.Kind() {
	case reflect.Ptr:
		return t.UpdateForce(value.Elem().Interface(), expr, fields, opts)
	case reflect.Struct:
		if tab, ok := data.(Table); ok {
			table = tab.TableName()
		} else {
			table = value.Type().Name()
		}
		paramNum := 0 //真正需要更新得字段数量
		for i := 0; i < value.NumField(); i++ {
			//类型
			fieldTypeStr := value.Field(i).Type().String()
			//属性名称
			fieldName := typeVal.Field(i).Name
			hasForce := false //是否是强制更新的字段
			if !value.Field(i).CanInterface() {
				continue
			}

			tag := value.Type().Field(i).Tag.Get("xsql")
			if tag == "" || tag == "-" || tag == "_" {
				continue
			}

			strs := strings.Split(tag, ",")
			//是否空值忽略该字段
			var omitempy bool
			if len(strs) > 1 {
				for _, s := range strs[1:] {
					if strings.Contains(s, "omitempty") {
						omitempy = true
					}
				}
			}

			valueFieldVal := ""
			if fieldTypeStr == "string" {
				valueFieldVal = fmt.Sprintf("%s", value.Field(i).Interface())
			} else if fieldTypeStr == "int" || fieldTypeStr == "int64" || fieldTypeStr == "int32" {
				valueFieldVal = fmt.Sprintf("%d", value.Field(i).Interface())
			}
			for _, field := range fields {
				if field == fieldName {
					hasForce = true
				}
			}
			//fmt.Println(value.Field(i).Type().String(),value.Field(i).Interface(),valueFieldVal)
			if omitempy && (valueFieldVal == "" || valueFieldVal == "0") && !hasForce {
				continue
			} else {
				paramNum++
				tag = strs[0]
				if placeholder == "?" {
					set = append(set, fmt.Sprintf("%s = %s", columnQuotes+tag+columnQuotes, placeholder))
				} else if placeholder == "@" {
					set = append(set, fmt.Sprintf("%s = @p%d", columnQuotes+tag+columnQuotes, paramNum))
				} else {
					set = append(set, fmt.Sprintf("%s = %s", columnQuotes+tag+columnQuotes, fmt.Sprintf(placeholder, i)))
				}
				// time特殊处理
				if value.Field(i).Type().String() == "time.Time" {
					ti := value.Field(i).Interface().(time.Time)
					bindArgs = append(bindArgs, ti.Format(timeLayout))
				} else {
					bindArgs = append(bindArgs, value.Field(i).Interface())
				}
			}

		}
		break
	default:
		return nil, errors.New("sql: only for struct type")
	}

	where := ""
	if expr != "" {
		where = fmt.Sprintf(` WHERE %s`, expr)
		//bindArgs = append(bindArgs, args...)
	}

	SQL := fmt.Sprintf(`UPDATE %s SET %s%s`, table, strings.Join(set, ", "), where)

	startTime := time.Now()
	res, err := t.Executor.Exec(SQL, bindArgs...)
	var rowsAffected int64
	if res != nil {
		rowsAffected, _ = res.RowsAffected()
	}
	l := &Log{
		Time:         time.Now().Sub(startTime),
		SQL:          SQL,
		Bindings:     bindArgs,
		RowsAffected: rowsAffected,
		Error:        err,
	}
	if debugFunc != nil {
		debugFunc(l)
	}
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (t *executor) Update(data interface{}, expr string, args []interface{}, opts *Options) (sql.Result, error) {
	placeholder := "?"
	if opts.Placeholder != "" {
		placeholder = opts.Placeholder
	}
	timeLayout := DefaultTimeLayout
	if opts.TimeLayout != "" {
		timeLayout = opts.TimeLayout
	}
	columnQuotes := "`"
	if opts.ColumnQuotes != "" {
		columnQuotes = opts.ColumnQuotes
	}
	var debugFunc DebugFunc
	if opts.DebugFunc != nil {
		debugFunc = opts.DebugFunc
	}

	set := make([]string, 0)
	bindArgs := make([]interface{}, 0)

	table := ""

	value := reflect.ValueOf(data)
	switch value.Kind() {
	case reflect.Ptr:
		return t.Update(value.Elem().Interface(), expr, args, opts)
	case reflect.Struct:
		if tab, ok := data.(Table); ok {
			table = tab.TableName()
		} else {
			table = value.Type().Name()
		}
		paramNum := 0 //真正需要更新得字段数量
		for i := 0; i < value.NumField(); i++ {
			fieldTypeStr := value.Field(i).Type().String()
			if !value.Field(i).CanInterface() {
				continue
			}

			tag := value.Type().Field(i).Tag.Get("xsql")
			if tag == "" || tag == "-" || tag == "_" {
				continue
			}

			strs := strings.Split(tag, ",")
			//是否空值忽略该字段
			var omitempy bool
			if len(strs) > 1 {
				for _, s := range strs[1:] {
					if strings.Contains(s, "omitempty") {
						omitempy = true
					}
				}
			}

			valueFieldVal := ""
			if fieldTypeStr == "string" {
				valueFieldVal = fmt.Sprintf("%s", value.Field(i).Interface())
			} else if fieldTypeStr == "int" || fieldTypeStr == "int64" || fieldTypeStr == "int32" || fieldTypeStr == "xsql.XsqlInt" {
				valueFieldVal = fmt.Sprintf("%d", value.Field(i).Interface())
			}

			//fmt.Println(value.Field(i).Type().String(),value.Field(i).Interface(),valueFieldVal)
			if omitempy && (valueFieldVal == "" || valueFieldVal == "0") && fieldTypeStr != "xsql.XsqlInt" {
				continue
			} else {
				paramNum++
				tag = strs[0]
				if placeholder == "?" {
					set = append(set, fmt.Sprintf("%s = %s", columnQuotes+tag+columnQuotes, placeholder))
				} else if placeholder == "@" {
					set = append(set, fmt.Sprintf("%s = @p%d", columnQuotes+tag+columnQuotes, paramNum))
				} else {
					set = append(set, fmt.Sprintf("%s = %s", columnQuotes+tag+columnQuotes, fmt.Sprintf(placeholder, i)))
				}
				// time特殊处理
				if value.Field(i).Type().String() == "time.Time" {
					ti := value.Field(i).Interface().(time.Time)
					bindArgs = append(bindArgs, ti.Format(timeLayout))
				} else {
					bindArgs = append(bindArgs, value.Field(i).Interface())
				}
			}

		}
		break
	default:
		return nil, errors.New("sql: only for struct type")
	}

	where := ""
	if expr != "" {
		where = fmt.Sprintf(` WHERE %s`, expr)
		bindArgs = append(bindArgs, args...)
	}

	SQL := fmt.Sprintf(`UPDATE %s SET %s%s`, table, strings.Join(set, ", "), where)

	startTime := time.Now()
	res, err := t.Executor.Exec(SQL, bindArgs...)
	var rowsAffected int64
	if res != nil {
		rowsAffected, _ = res.RowsAffected()
	}
	l := &Log{
		Time:         time.Now().Sub(startTime),
		SQL:          SQL,
		Bindings:     bindArgs,
		RowsAffected: rowsAffected,
		Error:        err,
	}
	if debugFunc != nil {
		debugFunc(l)
	}
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (t *executor) Save(data interface{}, orInsert bool, opts *Options) (sql.Result, error) {
	tt, ok := data.(TableAttribute)
	if !ok {
		return nil, errors.New("should implement an interface TableAttribute")
	}
	placeholder := "?"
	if opts.Placeholder != "" {
		placeholder = opts.Placeholder
	}
	timeLayout := DefaultTimeLayout
	if opts.TimeLayout != "" {
		timeLayout = opts.TimeLayout
	}
	columnQuotes := "`"
	if opts.ColumnQuotes != "" {
		columnQuotes = opts.ColumnQuotes
	}
	var debugFunc DebugFunc
	if opts.DebugFunc != nil {
		debugFunc = opts.DebugFunc
	}

	set := make([]string, 0)
	bindArgs := make([]interface{}, 0)

	table := ""

	//typeVal := reflect.TypeOf(data)
	value := reflect.ValueOf(data)
	//主键值 主键只能是int
	var primaryVal any
	switch value.Kind() {
	case reflect.Ptr:
		return t.Save(value.Elem().Interface(), orInsert, opts)
	case reflect.Struct:
		if tab, ok := data.(Table); ok {
			table = tab.TableName()
		} else {
			table = value.Type().Name()
		}
		paramNum := 0 //真正需要更新得字段数量

		for i := 0; i < value.NumField(); i++ {
			//类型
			//fieldTypeStr := value.Field(i).Type().String()
			//属性名称
			//fieldName := typeVal.Field(i).Name

			if !value.Field(i).CanInterface() {
				continue
			}

			tag := value.Type().Field(i).Tag.Get("xsql")
			if tag == "" || tag == "-" || tag == "_" {
				continue
			}

			strs := strings.Split(tag, ",")
			//是否空值忽略该字段
			//var omitempy bool
			//if len(strs) > 1 {
			//	for _, s := range strs[1:] {
			//		if strings.Contains(s, "omitempty") {
			//			omitempy = true
			//		}
			//	}
			//}
			tag = strs[0]
			if tag == tt.PrimaryName() {
				primaryVal = value.Field(i).Interface()
				if primaryVal == 0 {
					if !orInsert {
						return nil, errors.New("primary value zero!")
					}
					//如果没有数据则新增
					return t.Insert(data, opts)
				}
				continue
			}

			paramNum++

			if placeholder == "?" {
				set = append(set, fmt.Sprintf("%s = %s", columnQuotes+tag+columnQuotes, placeholder))
			} else if placeholder == "@" {
				set = append(set, fmt.Sprintf("%s = @p%d", columnQuotes+tag+columnQuotes, paramNum))
			} else {
				set = append(set, fmt.Sprintf("%s = %s", columnQuotes+tag+columnQuotes, fmt.Sprintf(placeholder, i)))
			}
			// time特殊处理
			if value.Field(i).Type().String() == "time.Time" {
				ti := value.Field(i).Interface().(time.Time)
				bindArgs = append(bindArgs, ti.Format(timeLayout))
			} else {
				bindArgs = append(bindArgs, value.Field(i).Interface())
			}

		}
		break
	default:
		return nil, errors.New("sql: only for struct type")
	}

	where := fmt.Sprintf(` WHERE %s = %d`, tt.PrimaryName(), primaryVal)

	SQL := fmt.Sprintf(`UPDATE %s SET %s%s`, table, strings.Join(set, ", "), where)

	startTime := time.Now()
	res, err := t.Executor.Exec(SQL, bindArgs...)
	var rowsAffected int64
	if res != nil {
		rowsAffected, _ = res.RowsAffected()
	}
	l := &Log{
		Time:         time.Now().Sub(startTime),
		SQL:          SQL,
		Bindings:     bindArgs,
		RowsAffected: rowsAffected,
		Error:        err,
	}
	if debugFunc != nil {
		debugFunc(l)
	}
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (t *executor) Exec(query string, args []interface{}, opts *Options) (sql.Result, error) {
	var debugFunc DebugFunc
	if opts.DebugFunc != nil {
		debugFunc = opts.DebugFunc
	}

	startTime := time.Now()
	res, err := t.Executor.Exec(query, args...)
	var rowsAffected int64
	if res != nil {
		rowsAffected, _ = res.RowsAffected()
	}
	l := &Log{
		Time:         time.Now().Sub(startTime),
		SQL:          query,
		Bindings:     args,
		RowsAffected: rowsAffected,
		Error:        err,
	}
	if debugFunc != nil {
		debugFunc(l)
	}
	if err != nil {
		return nil, err
	}

	return res, err
}
