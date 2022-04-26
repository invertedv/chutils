package sql

import (
	"database/sql"
	"fmt"
	"github.com/invertedv/chutils"
	"log"
	"strconv"
	"strings"
)

type Reader struct {
	Sql string // Sql is the SELECT string.  It does not have an INSERT
	DB  *sql.DB
	//	Table string
	data      *sql.Rows
	TableSpec *chutils.TableDef // TableDef is the table def for the file.  Can be supplied or derived from the file

}

func NewReader(sql string, db *sql.DB) *Reader {
	return &Reader{
		Sql:  sql,
		DB:   db,
		data: nil,
		TableSpec: &chutils.TableDef{
			Name:      "",
			Key:       "",
			Engine:    chutils.MergeTree,
			FieldDefs: nil,
		},
	}
}

func (r *Reader) Init() error {

	if r.Sql == "" {
		return chutils.NewChErr(chutils.ErrSQL)
	}
	qry := "SELECT * FROM (" + r.Sql + ") LIMIT 1"
	rows, err := r.DB.Query(qry)
	defer rows.Close()

	if err != nil {
		return err
	}
	ct, err := rows.ColumnTypes()
	if err != nil {
		return nil
	}

	fds := make(map[int]*chutils.FieldDef)
	for ind, c := range ct {
		chf := chutils.ChField{
			Base:       0,
			Length:     0,
			OuterFunc:  "",
			DateFormat: "",
		}
		tn := c.DatabaseTypeName()
		if strings.Index(tn, "Nullable") >= 0 {
			chf.OuterFunc = "Nullable"
		}
		if strings.Index(tn, "LowCardinality") >= 0 {
			chf.OuterFunc = "LowCardinality"
		}
		types := []string{"Date", "Int", "Float", "FixedString", "String"}
		chtypes := []chutils.ChType{chutils.Date, chutils.Int, chutils.Float, chutils.FixedString, chutils.String}
		var trailing string
		for i, t := range types {
			if indx := strings.Index(tn, t); indx >= 0 {
				chf.Base = chtypes[i]
				trailing = tn[indx+len(t):]
				break
			}
		}
		switch chf.Base {
		case chutils.Int, chutils.Float:
			l, err := strconv.ParseInt(trailing, 10, 32)
			if err != nil {
				return err
			}
			chf.Length = int(l)
		case chutils.FixedString:
			l, err := strconv.ParseInt(trailing[1:len(trailing)-2], 10, 32)
			if err != nil {
				return err
			}
			chf.Length = int(l)
		}

		fd := &chutils.FieldDef{
			Name:        c.Name(),
			ChSpec:      chf,
			Description: "",
			Legal:       &chutils.LegalValues{},
		}
		fds[ind] = fd
	}
	r.TableSpec.FieldDefs = fds
	return nil
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

func (rdr *Reader) Insert() error {
	qry := fmt.Sprintf("INSERT INTO %s %s", rdr.TableSpec.Name, rdr.Sql)
	if _, err := rdr.DB.Exec(qry); err != nil {
		log.Fatalln(err)
	}
	return nil
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

//TODO: make separator "," and eol "" ???
func NewWriter(table string, db *sql.DB, separator string, eol string) *Writer {
	return &Writer{Table: table,
		DB:        db,
		hold:      append(make([]byte, 0), '('),
		separator: ",",
		eol:       "",
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

// TODO: think about whether the TableDef should have a Name
