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

// Reader implements chutils.Input interface.
type Reader struct {
	Sql       string            // Sql is the SELECT string.  It does not have an INSERT
	RowsRead  int               // RowsRead is the number of rows read so far
	TableSpec *chutils.TableDef // TableDef is the table def for the file.  Can be supplied or derived from the file.
	Name      string            // Name is the name of the output table created by Insert()
	conn      *chutils.Connect  // conn is connector to ClickHouse
	data      *sql.Rows         // data is the output of executing Reader.Sql
}

// NewReader creates a new reader.
func NewReader(sql string, conn *chutils.Connect) *Reader {
	return &Reader{
		Sql:      sql,
		conn:     conn,
		RowsRead: 0,
		Name:     "",
		data:     nil,
		TableSpec: &chutils.TableDef{
			Key:       "",
			Engine:    chutils.MergeTree,
			FieldDefs: nil,
		},
	}
}

// Init initializes Reader.TableDef by looking at the output of the query
func (rdr *Reader) Init() error {
	var rows *sql.Rows
	var e error = nil
	if rdr.Sql == "" {
		return chutils.Wrapper(chutils.ErrSQL, "no sql statement")
	}
	qry := "SELECT * FROM (" + rdr.Sql + ") LIMIT 1"
	rows, err := rdr.conn.Query(qry)
	if err != nil {
		return err
	}
	defer func() { e = rows.Close() }()
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
	return e
}

// Read reads nTarget rows.  If validate is true, the fields are validated against the TableDef.
// If validate is false, the fields are returned as strings.
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
			if e := rdr.Reset(); e != nil {
				return nil, e
			}
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
				outValue, _ := rdr.TableSpec.FieldDefs[ind].Validator(row[ind], rdr.TableSpec, row, chutils.VPending)
				row[ind] = outValue
			}
		}
		data = append(data, row)
	}
	return
}

// Reset resets the result set. The next read returns the first record.
func (rdr *Reader) Reset() error {
	if rdr.data != nil {
		if e := rdr.data.Close(); e != nil {
			return e
		}
	}
	var e error
	if rdr.data, e = rdr.conn.Query(rdr.Sql); e != nil {
		return e
	}
	rdr.RowsRead = 0
	return nil
}

// CountLines returns the number of rows in the result set.
func (rdr *Reader) CountLines() (numLines int, err error) {
	var res *sql.Rows
	numLines = 0
	qry := fmt.Sprintf("SELECT COUNT(*) AS n FROM (%s)", rdr.Sql)
	if res, err = rdr.conn.Query(qry); err != nil {
		return
	}
	defer func() { err = res.Close() }()
	for res.Next() {
		if err = res.Scan(&numLines); err != nil {
			numLines = 0
		}
		return
	}
	return
}

// Seek moves to the lineNo record of the result set.  The next read will start there.
func (rdr *Reader) Seek(lineNo int) error {
	if err := rdr.Reset(); err != nil {
		return err
	}
	for cnt := 0; cnt < lineNo-1; cnt++ {
		if !rdr.data.Next() {
			return chutils.Wrapper(chutils.ErrSQL, "seek past end of table")
		}
	}
	return nil
}

// Close closes the result set
func (rdr *Reader) Close() error {
	if rdr.data == nil {
		return nil
	}
	rdr.RowsRead = 0
	return rdr.data.Close()
}

// Insert executes Reader.Sql and inserts the result into Reader.Name
func (rdr *Reader) Insert() error {
	qry := fmt.Sprintf("INSERT INTO %s %s", rdr.Name, rdr.Sql)
	if _, err := rdr.conn.Exec(qry); err != nil {
		log.Fatalln(err)
	}
	return nil
}

// Writer implements chutils.Output
type Writer struct {
	Table     string           // Table is the output table
	separator rune             // separator for values.  This is set to ','
	eol       rune             // EOL. This is set to 0.
	conn      *chutils.Connect // conn is the connector to ClickHouse
	hold      []byte           // holds the Value statements as they are built.
}

// Write writes the byte slice to Writer.hold. The byte slice is a single row of the output
// table with the fields being comma separated.
func (wtr *Writer) Write(b []byte) (n int, err error) {
	n = len(b)
	if len(wtr.hold) > 1 {
		wtr.hold = append(wtr.hold, ')', ',', '(')
	}
	wtr.hold = append(wtr.hold, b...)
	return n, nil
}

// Separator returns a comma rune.  This method is needed by chutils.Export.
func (wtr *Writer) Separator() rune {
	return wtr.separator
}

// EOL returns 0.  This method is needed by chutils.Export.
func (wtr *Writer) EOL() rune {
	return wtr.eol
}

// Insert executes an Insert query -- the values must have been built using Writer.Write
func (wtr *Writer) Insert() error {
	if wtr.Table == "" {
		return chutils.Wrapper(chutils.ErrSQL, "no table name")
	}
	qry := fmt.Sprintf("INSERT INTO %s VALUES", wtr.Table) + string(wtr.hold) + ")"
	_, err := wtr.conn.Exec(qry)
	return err
}

// Close closes the work on the Values so far--that is, it empties the buffer.
func (wtr *Writer) Close() error {
	wtr.hold = make([]byte, 0)
	wtr.hold = append(wtr.hold, '(')
	return nil
}

// Name returns the name of the table created by Insert.
func (wtr *Writer) Name() string {
	return wtr.Table
}

// NewWriter creates a new SQL writer
func NewWriter(table string, conn *chutils.Connect) *Writer {
	return &Writer{Table: table,
		conn:      conn,
		hold:      append(make([]byte, 0), '('),
		separator: ',',
		eol:       0,
	}
}

// Wrtrs creates an array of writers suitable for chutils.Concur
func Wrtrs(table string, nWrtr int, conn *chutils.Connect) (wrtrs []chutils.Output, err error) {

	wrtrs = nil
	err = nil

	for ind := 0; ind < nWrtr; ind++ {
		a := NewWriter(table, conn)
		wrtrs = append(wrtrs, a)
	}
	return
}
