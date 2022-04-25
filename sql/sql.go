package sql

import (
	"database/sql"
	"fmt"
	"github.com/invertedv/chutils"
)

type Reader struct {
	Sql   string // Sql is the SELECT string.  It does not have an INSERT
	DB    *sql.DB
	Table string
	data  *sql.Rows
}

func (rdr *Reader) Read(nTarget int, validate bool) (data []chutils.Row, err error) {

	return nil, nil
}

func (rdr *Reader) Reset() {
	return
}

func (rdr *Reader) CountLines() (numLines int, err error) {
	return 0, nil
}

func (rdr *Reader) Seek(lineNo int) error {
	return nil
}

func (rdr *Reader) Name() string {
	return "My Name"
}

func (rdr *Reader) Close() error {
	if rdr.data == nil {
		return nil
	}
	return rdr.data.Close()
}

func NewReader(sql string, table string, db *sql.DB) *Reader {
	return &Reader{Sql: sql, DB: db, Table: table, data: nil}
}

type Writer struct {
	Table     string
	separator string
	eol       string
	DB        *sql.DB
	hold      []byte
}

func (w *Writer) Write(b []byte) (n int, err error) {
	n = len(b)
	if len(w.hold) > 1 {
		w.hold = append(w.hold, ')', ',', '(')
	}
	w.hold = append(w.hold, b...)
	return n, nil
}

func (w *Writer) Separator() string {
	return w.separator
}

func (w *Writer) EOL() string {
	return w.eol
}

func (w *Writer) Insert() error {
	qry := fmt.Sprintf("Insert into %s Values", w.Table) + string(w.hold) + ")"
	_, err := w.DB.Exec(qry)
	if err == nil {
		w.hold = make([]byte, 0)
		w.hold = append(w.hold, '(')
	}
	return err
}

func (w *Writer) Close() error {
	w.hold = make([]byte, 0)
	w.hold = append(w.hold, '(')
	return nil
}

func (w *Writer) Name() string {
	return w.Table
}

func NewWriter(table string, db *sql.DB, separator string, eol string) *Writer {
	return &Writer{Table: table,
		DB:        db,
		hold:      append(make([]byte, 0), '('),
		separator: separator,
		eol:       eol,
	}
}

func Wrtrs(table string, nWrtr int, db *sql.DB, separator string, eol string) (wrtrs []chutils.Output, err error) {

	wrtrs = nil // make([]*os.File, 0)
	err = nil

	for ind := 0; ind < nWrtr; ind++ {
		a := NewWriter(table, db, separator, eol)
		wrtrs = append(wrtrs, a)
	}
	return
}

func Load(rdr chutils.Input, wrtr chutils.Output) (err error) {
	err = chutils.Export(rdr, wrtr, ",", "")
	if err != nil {
		return
	}
	err = wrtr.Insert()
	//	err = InsertFile(table, wrtr.Name(), '|', CSV, "", con)
	return
}
