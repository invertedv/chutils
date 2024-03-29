// Package sql implements Input/Output for SQL.
// One can use sql to create a new table from a query.  This is similar to the
// ClickHouse CREATE MATERIALIZED VIEW statement but there is no trigger to update the output table if the input changes.
//
// There are three approaches to creating a new ClickHouse table:
//
//   - Direct ClickHouse insertion.  Use sql Reader.Insert to issue an Insert query with Reader.SQL as the source.
//
//   - Values insertion. Use sql Writer.Insert to issue an Insert query using VALUES. The values are created by
//     sql Writer writing values from a reader.  Although the source can be a sql.Reader, more commonly one would
//     expect it to be a file.Reader.
//
//   - clickhouse-client insert.  Use a file Writer.Insert to create a CSV file and then issue a shell command to run
//     the clickhouse-client to insert the file.
//
// Before any of these approaches are used, the TableDef.CreateTable() can be used to create the destination table.
package sql

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/invertedv/chutils"
)

// Reader implements chutils.Input interface.
type Reader struct {
	SQL       string            // SQL is the SELECT string.  It does not have an INSERT
	RowsRead  int               // RowsRead is the number of rows read so far
	Name      string            // Name is the name of the output table created by Insert()
	tableSpec *chutils.TableDef // TableDef is the table def for the file.  Can be supplied or derived from the file.
	conn      *chutils.Connect  // conn is connector to ClickHouse
	data      *sql.Rows         // data is the output of executing Reader.SQL
}

// NewReader creates a new reader.
func NewReader(sqlStr string, conn *chutils.Connect) *Reader {
	return &Reader{
		SQL:      sqlStr,
		conn:     conn,
		RowsRead: 0,
		Name:     "",
		data:     nil,
		tableSpec: &chutils.TableDef{
			Key:       "",
			Engine:    chutils.MergeTree,
			FieldDefs: nil,
		},
	}
}

// randUnifInt generates a slice whose elements are random U[0,upper) int64's
func randUnifInt(n, upper int) ([]int64, error) {
	const bytesPerInt = 8

	// generate random bytes
	b1 := make([]byte, bytesPerInt*n)
	if _, e := rand.Read(b1); e != nil {
		return nil, e
	}

	outInts := make([]int64, n)
	rdr := bytes.NewReader(b1)

	for ind := 0; ind < n; ind++ {
		r, e := rand.Int(rdr, big.NewInt(int64(upper)))
		if e != nil {
			return nil, e
		}
		outInts[ind] = r.Int64()
	}

	return outInts, nil
}

// randomLetters generates a string of length "length" by randomly choosing from a-z
// This is copied from package utilities, but cannot import as it would be circular.
func randomLetters(length int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"

	randN, err := randUnifInt(len(letters), len(letters))
	if err != nil {
		panic(err)
	}

	name := ""
	for ind := 0; ind < length; ind++ {
		name += letters[randN[ind] : randN[ind]+1]
	}

	return name
}
func (rdr *Reader) TableSpec() *chutils.TableDef {
	return rdr.tableSpec
}

// Init initializes Reader.TableDef by looking at the output of the query.
// if key is empty, it defaults to the first field.
func (rdr *Reader) Init(key string, engine chutils.EngineType) (err error) {
	var rows *sql.Rows
	if rdr.SQL == "" {
		return chutils.Wrapper(chutils.ErrSQL, "no sql statement")
	}
	qry := "SELECT * FROM (" + rdr.SQL + ") LIMIT 1"

	id := randomLetters(10)
	qOpt := clickhouse.WithQueryID(id)
	ctx := context.Background()
	ctx = clickhouse.Context(ctx, qOpt)
	if rows, err = rdr.conn.QueryContext(ctx, qry); err != nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	ct, err := rows.ColumnTypes()
	if err != nil {
		return
	}

	// work through column types to build slice of FieldDefs
	fds := make(map[int]*chutils.FieldDef)
	for ind, c := range ct {
		chf := chutils.ChField{
			Base:   0,
			Length: 0,
			Funcs:  nil,
			Format: "",
		}
		// parse DataBaseTypeName
		tn := c.DatabaseTypeName()

		if strings.Contains(tn, "Array") {
			chf.Funcs = append(chf.Funcs, chutils.OuterArray)
			tn = tn[6 : len(tn)-1]
		}

		if strings.Contains(tn, "Nullable") {
			chf.Funcs = append(chf.Funcs, chutils.OuterNullable)
			tn = tn[9 : len(tn)-1]
		}

		if strings.Contains(tn, "LowCardinality") {
			chf.Funcs = append(chf.Funcs, chutils.OuterLowCardinality)
			tn = tn[15 : len(tn)-1]
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
			chf.Format = time.RFC3339
		case chutils.ChInt, chutils.ChFloat:
			var l int64
			if l, err = strconv.ParseInt(trailing, 10, 32); err != nil {
				return
			}
			chf.Length = int(l)
		case chutils.ChFixedString:
			var l int64
			// strip off leading/trailing parens
			for len(trailing) > 0 && trailing[0] == '(' {
				trailing = trailing[1:]
			}
			for len(trailing) > 0 && trailing[len(trailing)-1] == ')' {
				trailing = trailing[:len(trailing)-1]
			}
			if l, err = strconv.ParseInt(trailing, 10, 32); err != nil {
				return
			}
			chf.Length = int(l)
		}

		name := c.Name()
		if i := strings.Index(name, "."); i > 0 {
			name = name[i+1:]
		}

		fd := &chutils.FieldDef{
			Name:        name,
			ChSpec:      chf,
			Description: "",
			Legal:       chutils.NewLegalValues(),
		}

		fds[ind] = fd
	}

	//	if key is empty, make it the first field
	if key == "" {
		key = fds[0].Name
	}

	rdr.tableSpec = chutils.NewTableDef(key, engine, fds)

	_, _ = rdr.conn.Exec(fmt.Sprintf("KILL QUERY WHERE query_id = '%s'", id))
	return rdr.TableSpec().Check()
}

// Must type the interface for Scan to correctly read arrays
func typer(inType string) (ii interface{}) {
	switch inType {
	case "[]float32":
		return make([]float32, 0)
	case "[]float64":
		return make([]float64, 0)
	case "[]int32":
		return make([]int32, 0)
	case "[]int64":
		return make([]int64, 0)
	case "[]string":
		return make([]string, 0)
	case "[]time.Time":
		return make([]time.Time, 0)
	}
	return
}

// Read reads nTarget rows.  If nTarget == 0, the entire result set is returned.
//
// If validation == true:
//   - The data is validated according to the rules in rdr.TableSpec.
//   - The results are returned as the slice valid.
//   - data is returned with the fields appropriately typed.
//
// If validation == false:
//   - data is returned with the fields appropriately typed.
//   - The return slice valid is nil
//
// err is io.EOF at the end of the record set
func (rdr *Reader) Read(nTarget int, validate bool) (data []chutils.Row, valid []chutils.Valid, err error) {
	data = nil
	valid = nil
	if nTarget == 0 {
		nTarget, err = rdr.CountLines()
		if err != nil {
			return nil, nil, err
		}
	}

	if rdr.TableSpec() == nil {
		return nil, nil, chutils.Wrapper(chutils.ErrFields, "must run Init before read")
	}

	if rdr.data == nil {
		if rdr.data, err = rdr.conn.Query(rdr.SQL); err != nil {
			return nil, nil, err
		}
	}

	cols, err := rdr.data.Columns()
	if err != nil {
		return
	}

	ncols := len(cols)

	var values = make([]interface{}, ncols)
	ct, _ := rdr.data.ColumnTypes()

	for i, c := range ct {
		ii := typer(c.ScanType().String())
		values[i] = &ii
	}

	for rowCount := 0; rowCount < nTarget; rowCount++ {
		if !rdr.data.Next() {
			return data, valid, io.EOF
		}
		rdr.RowsRead++

		if err = rdr.data.Scan(values...); err != nil {
			return
		}

		row := make(chutils.Row, len(values))

		for ind := 0; ind < len(values); ind++ {
			row[ind] = *(values[ind].(*interface{}))
		}

		if validate && rdr.TableSpec().FieldDefs != nil {
			vrow := make(chutils.Valid, ncols)
			for ind := 0; ind < ncols; ind++ {
				outValue, stat := rdr.TableSpec().FieldDefs[ind].Validator(row[ind])
				row[ind] = outValue
				vrow[ind] = stat
			}
			valid = append(valid, vrow)
		}

		data = append(data, row)
	}

	return data, valid, err
}

// Reset resets the result set. The next read returns the first record.
func (rdr *Reader) Reset() error {
	if rdr.data != nil {
		if e := rdr.data.Close(); e != nil {
			return e
		}
	}

	var e error
	if rdr.data, e = rdr.conn.Query(rdr.SQL); e != nil {
		return e
	}

	rdr.RowsRead = 0

	return nil
}

// CountLines returns the number of rows in the result set.
func (rdr *Reader) CountLines() (numLines int, err error) {
	var res *sql.Rows
	numLines = 0
	qry := fmt.Sprintf("SELECT COUNT(*) AS n FROM (%s)", rdr.SQL)
	if res, err = rdr.conn.Query(qry); err != nil {
		return
	}
	defer func() { err = res.Close() }()

	for res.Next() {
		if err = res.Scan(&numLines); err != nil {
			numLines = 0
		}
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
			return chutils.Wrapper(chutils.ErrSeek, "seek past end of table")
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

// Insert executes Reader.SQL and inserts the result into Reader.Name
func (rdr *Reader) Insert() error {
	if rdr.Name == "" {
		return chutils.Wrapper(chutils.ErrSQL, "Reader Name field is empty")
	}

	qry := fmt.Sprintf("INSERT INTO %s %s", rdr.Name, rdr.SQL)

	return rdr.conn.Execute(qry)
}

func (rdr *Reader) Materialize(orderBy string) error {
	if rdr.Name == "" {
		return chutils.Wrapper(chutils.ErrSQL, "Reader Name field is empty")
	}

	qry := fmt.Sprintf("DROP TABLE IF EXISTS %s", rdr.Name)
	if e := rdr.conn.Execute(qry); e != nil {
		return e
	}

	qry = fmt.Sprintf("CREATE MATERIALIZED VIEW %s ENGINE=MergeTree ORDER BY %s POPULATE AS %s", rdr.Name, orderBy, rdr.SQL)

	return rdr.conn.Execute(qry)
}

// Writer implements chutils.Output
type Writer struct {
	Table     string           // Table is the output table
	separator rune             // separator for values.  This is set to ','
	eol       rune             // eol. This is set to 0.
	text      string           // text is the text qualifer: defaults to ' for ClickHouse
	conn      *chutils.Connect // conn is the connector to ClickHouse
	hold      []byte           // holds the Value statements as they are built.
}

// Write writes the byte slice to Writer.hold. The byte slice is a single row of the output
func (wtr *Writer) Write(b []byte) (n int, err error) {
	n = len(b)

	if len(wtr.hold) > 1 {
		wtr.hold = append(wtr.hold, ')', byte(wtr.Separator()), '(')
	}

	wtr.hold = append(wtr.hold, b...)

	return n, nil
}

// Text returns the string delimiter
func (wtr *Writer) Text() string {
	return wtr.text
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

	// Execute query with timeout parameter (if supplied)
	if err := wtr.conn.Execute(qry); err != nil {
		_ = wtr.Close()
		return err
	}

	return wtr.Close()
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
		text:      "'",
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
