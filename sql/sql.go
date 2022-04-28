package sql

import (
	"database/sql"
	"fmt"
	"github.com/invertedv/chutils"
	"io"
	"log"
	"strconv"
	"strings"
	"time"
)

type Reader struct {
	Sql      string // Sql is the SELECT string.  It does not have an INSERT
	conn     *chutils.Connect
	RowsRead int
	//	Table string
	data      *sql.Rows
	TableSpec *chutils.TableDef // TableDef is the table def for the file.  Can be supplied or derived from the file
}

func NewReader(sql string, conn *chutils.Connect) *Reader {
	return &Reader{
		Sql:      sql,
		conn:     conn,
		RowsRead: 0,
		data:     nil,
		TableSpec: &chutils.TableDef{
			//			Name:      "",
			Key:       "",
			Engine:    chutils.MergeTree,
			FieldDefs: nil,
		},
	}
}

func (rdr *Reader) Init() error {

	if rdr.Sql == "" {
		return chutils.Wrapper(chutils.ErrSQL, "no sql statement")
	}
	qry := "SELECT * FROM (" + rdr.Sql + ") LIMIT 1"
	rows, err := rdr.conn.Query(qry)
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
		chtypes := []chutils.ChType{chutils.ChDate, chutils.ChInt, chutils.ChFloat, chutils.ChFixedString, chutils.ChString}
		var trailing string
		for i, t := range types {
			if indx := strings.Index(tn, t); indx >= 0 {
				chf.Base = chtypes[i]
				trailing = tn[indx+len(t):]
				break
			}
		}
		switch chf.Base {
		case chutils.ChDate:
			// ClickHouse connector brings in dates with this format
			chf.DateFormat = time.RFC3339
		case chutils.ChInt, chutils.ChFloat:
			l, err := strconv.ParseInt(trailing, 10, 32)
			if err != nil {
				return err
			}
			chf.Length = int(l)
		case chutils.ChFixedString:
			// strip off leading/trailing parens
			for len(trailing) > 0 && trailing[0] == '(' {
				trailing = trailing[1:]
			}
			for len(trailing) > 0 && trailing[len(trailing)-1] == ')' {
				trailing = trailing[:len(trailing)-1]
			}
			l, err := strconv.ParseInt(trailing, 10, 32)
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
	rdr.TableSpec.FieldDefs = fds
	return nil
}

func (rdr *Reader) Read(nTarget int, validate bool) (data []chutils.Row, err error) {
	data = nil
	if rdr.data == nil {
		if rdr.data, err = rdr.conn.Query(rdr.Sql); err != nil {
			return nil, err
		}
	}
	cols, err := rdr.data.Columns()
	if err != nil {
		return
	}
	ncols := len(cols)

	for rowCount := 0; rowCount < nTarget; rowCount++ {
		t := make([]interface{}, ncols)
		for ind := 0; ind < ncols; ind++ {
			t[ind] = new(sql.RawBytes)
		}
		if !rdr.data.Next() {
			rdr.Reset()
			return data, io.EOF
		}
		rdr.RowsRead++
		err = rdr.data.Scan(t...)
		if err != nil {
			return
		}
		row := make(chutils.Row, 0)
		for ind := 0; ind < ncols; ind++ {
			l := string(*t[ind].(*sql.RawBytes))
			//			if ind := strings.Index(l, "T00:00:00Z"); ind > 0 {
			//				l = l[:ind]
			//			}
			row = append(row, l)
		}
		if validate && rdr.TableSpec.FieldDefs != nil {
			for ind := 0; ind < ncols; ind++ {
				outValue, _ := rdr.TableSpec.FieldDefs[ind].Validator(row[ind], rdr.TableSpec, row, chutils.Pending)
				row[ind] = outValue
			}
		}
		data = append(data, row)
	}
	return
}

func (rdr *Reader) Reset() {
	if rdr.data != nil {
		rdr.data.Close()
	}
	var e error
	if rdr.data, e = rdr.conn.Query(rdr.Sql); e != nil {
		return
	}
	rdr.RowsRead = 0
	return
}

func (rdr *Reader) CountLines() (numLines int, err error) {
	var res *sql.Rows
	numLines = 0
	qry := fmt.Sprintf("SELECT COUNT(*) AS n FROM (%s)", rdr.Sql)
	if res, err = rdr.conn.Query(qry); err != nil {
		return
	}
	defer res.Close()
	for res.Next() {
		if err = res.Scan(&numLines); err != nil {
			numLines = 0
		}
		return
	}
	return
}

func (rdr *Reader) Seek(lineNo int) error {
	rdr.Reset()
	for cnt := 0; cnt < lineNo-1; cnt++ {
		if !rdr.data.Next() {
			return chutils.Wrapper(chutils.ErrSQL, "seek past end of table")
		}
	}
	return nil
}

func (rdr *Reader) Name() string {
	return "N/A"
}

func (rdr *Reader) Close() error {
	if rdr.data == nil {
		return nil
	}
	rdr.RowsRead = 0
	return rdr.data.Close()
}

func (rdr *Reader) Insert(table string) error {
	qry := fmt.Sprintf("INSERT INTO %s %s", table, rdr.Sql)
	if _, err := rdr.conn.Exec(qry); err != nil {
		log.Fatalln(err)
	}
	return nil
}

type Writer struct {
	Table     string
	separator string
	eol       string
	conn      *chutils.Connect
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
	if w.Table == "" {
		return chutils.Wrapper(chutils.ErrSQL, "no table name")
	}
	qry := fmt.Sprintf("Insert into %s Values", w.Table) + string(w.hold) + ")"
	_, err := w.conn.Exec(qry)
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

// NewWriter creates a new SQL writer
func NewWriter(table string, conn *chutils.Connect) *Writer {
	return &Writer{Table: table,
		conn:      conn,
		hold:      append(make([]byte, 0), '('),
		separator: ",",
		eol:       "",
	}
}

func Wrtrs(table string, nWrtr int, conn *chutils.Connect) (wrtrs []chutils.Output, err error) {

	wrtrs = nil
	err = nil

	for ind := 0; ind < nWrtr; ind++ {
		a := NewWriter(table, conn)
		wrtrs = append(wrtrs, a)
	}
	return
}
