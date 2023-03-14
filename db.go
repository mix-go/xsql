package xsql

import (
	"database/sql"
	"errors"
	"fmt"
)

var DefaultTimeLayout = "2006-01-02 15:04:05"

// DefaultTimeFunc
// mysql: return placeholder
// oracle: return fmt.Sprintf("TO_TIMESTAMP(%s, 'SYYYY-MM-DD HH24:MI:SS:FF6')", placeholder)
var DefaultTimeFunc = func(placeholder string) string {
	return placeholder
}

type QueryRes struct {
	InsertId int64
	Affected int64
	sql.Result
}

func (this QueryRes) LastInsertId() (int64, error) {
	return this.InsertId, nil
}

func (this QueryRes) RowsAffected() (int64, error) {
	return this.Affected, nil
}

type TimeFunc func(placeholder string) string

type DB struct {
	Options  Options
	raw      *sql.DB
	executor executor
	query    query
}

// New
// opts 以最后一个为准
func New(db *sql.DB, opts ...Options) *DB {
	o := Options{}
	for _, v := range opts {
		o = v
	}
	return &DB{
		Options: o,
		raw:     db,
		executor: executor{
			Executor: db,
		},
		query: query{
			Query: db,
		},
	}
}
func (t *DB) GetRawDB() *sql.DB {
	return t.raw
}
func (t *DB) Insert(data interface{}, opts ...Options) (sql.Result, error) {
	for _, o := range opts {
		t.Options.InsertKey = o.InsertKey
	}
	return t.executor.Insert(data, &t.Options)
}

// 返回最后插入的ID
func (t *DB) InsertTakeLastId(data interface{}, withSeq string, opts ...Options) (sql.Result, error) {
	for _, o := range opts {
		t.Options.InsertKey = o.InsertKey
	}

	qr, err := t.executor.InsertTakeLastId(data, withSeq, t.query, &t.Options)
	if err != nil {
		return nil, err
	}
	table, _ := data.(Table)
	if table.DBType() == "Oracle" {
		rows, err := t.GetLastId(data, withSeq)
		if err != nil {
			return nil, err
		}
		qr = QueryRes{
			InsertId: rows[0].Get("INSERT_ID").Int(),
			Affected: 1,
		}
		return qr, nil
	}
	return qr, nil
}

func (t *DB) BatchInsert(data interface{}, opts ...Options) (sql.Result, error) {
	for _, o := range opts {
		t.Options.InsertKey = o.InsertKey
	}
	return t.executor.BatchInsert(data, &t.Options)
}

func (t *DB) Update(data interface{}, expr string, args ...interface{}) (sql.Result, error) {
	return t.executor.Update(data, expr, args, &t.Options)
}

// 强制更新值为空的字段
func (t *DB) UpdateForce(data interface{}, expr string, fields ...string) (sql.Result, error) {
	return t.executor.UpdateForce(data, expr, fields, &t.Options)
}

func (t *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return t.executor.Exec(query, args, &t.Options)
}

func (t *DB) Begin() (*Tx, error) {
	tx, err := t.raw.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{
		raw: tx,
		DB: &DB{
			Options: t.Options,
			executor: executor{
				Executor: tx,
			},
			query: query{
				Query: tx,
			},
		},
	}, nil
}

func (t *DB) Query(query string, args ...interface{}) ([]Row, error) {
	f, err := t.query.Fetch(query, args, &t.Options)
	if err != nil {
		return nil, err
	}
	r, err := f.Rows()
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (t *DB) Find(i interface{}, query string, args ...interface{}) error {
	f, err := t.query.Fetch(query, args, &t.Options)
	if err != nil {
		return err
	}
	if err := f.Find(i); err != nil {
		return err
	}
	return nil
}

func (t *DB) First(i interface{}, query string, args ...interface{}) error {
	f, err := t.query.Fetch(query, args, &t.Options)
	if err != nil {
		return err
	}
	if err := f.First(i); err != nil {
		return err
	}
	return nil
}

func (t *DB) GetLastId(data any, seq string) ([]Row, error) {
	table, _ := data.(Table)
	switch table.DBType() {
	case "Oracle":
		sqlStr := fmt.Sprintf(`SELECT %s.CURRVAL INSERT_ID FROM DUAL`, seq)
		return t.Query(sqlStr)
	case "Mssql":
		sqlStr := fmt.Sprintf(`Select SCOPE_IDENTITY() INSERT_ID`)
		return t.Query(sqlStr)

	}
	return nil, errors.New("未查到序列自增值")
}
