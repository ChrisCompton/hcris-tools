package hcris

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

var DB *sql.DB

func SQLiteConnect(file string) error {
	var err error
	DB, err = sql.Open("sqlite3", file)
	Check(err)
	return err
}

func SQLiteClose() error {
	err := DB.Close()
	return err
}

func SQLiteExecute(sql string) (int64, error) {
	DebugVerbose("Start SQLiteExecute")

	stmt, err := DB.Prepare(sql)
	Check(err)

	res, err := stmt.Exec()
	Check(err)

	affect, err := res.RowsAffected()
	Check(err)

	DebugVerbose("End SQLiteExecute")

	return affect, err
}
