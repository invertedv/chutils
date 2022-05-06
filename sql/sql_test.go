package sql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/invertedv/chutils"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestReader_Init(t *testing.T) {
	var db *sql.DB
	var mock sqlmock.Sqlmock
	var err error = nil
	con := &chutils.Connect{"", "", "", "", db}
	con.DB, mock, err = sqlmock.New()
	if err != nil {
		return
	}

	// result names & types
	resNames := []string{"astr", "bflt", "cfstr", "dint", "edate"}
	resBases := []chutils.ChType{chutils.ChString, chutils.ChFloat, chutils.ChFixedString, chutils.ChInt, chutils.ChDate}

	// for sqlmock
	coltype := []string{"String", "Float64", "FixedString(1)", "Int16", "Date"}
	examples := []interface{}{"hello", 1.0, "X", 1, "2022-01-02"}
	// data
	dv := []driver.Value{"a", 1.0, "X", 1, "2022-01-02"}

	// build columns
	cols := make([]*sqlmock.Column, 0)
	for j := 0; j < len(resBases); j++ {
		tmp := sqlmock.NewColumn(resNames[j]).OfType(coltype[j], examples[j])
		cols = append(cols, tmp)
	}
	rows := sqlmock.NewRowsWithColumnDefinition(cols...).AddRow(dv...)

	// query issued by rdr.Init
	mock.ExpectQuery(`^SELECT \* FROM \(SELECT \* FROM bbb\) LIMIT 1`).WillReturnRows(rows)

	rdr := NewReader("SELECT * FROM bbb", con)
	if err := rdr.Init(); err != nil {
		t.Errorf("incorrect query")
	}
	actNames := make([]string, 0)
	actBases := make([]chutils.ChType, 0)
	for j := 0; j < len(rdr.TableSpec().FieldDefs); j++ {
		_, fd, _ := rdr.TableSpec().Get(resNames[j])
		actNames = append(actNames, fd.Name)
		actBases = append(actBases, fd.ChSpec.Base)
	}
	assert.Equal(t, resNames, actNames)
	assert.Equal(t, resBases, actBases)

}

func TestReader_Read(t *testing.T) {
	dtMiss := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	var db *sql.DB
	var mock sqlmock.Sqlmock
	var err error = nil
	con := &chutils.Connect{"", "", "", "", db}
	con.DB, mock, err = sqlmock.New()
	if err != nil {
		return
	}
	// result names & types
	resNames := []string{"astr", "bflt", "cfstr", "dint", "edate"}
	resBases := []chutils.ChType{chutils.ChString, chutils.ChFloat, chutils.ChFixedString, chutils.ChInt, chutils.ChDate}

	// for sqlmock
	coltype := []string{"String", "Float64", "FixedString(1)", "Int16", "Date"}
	examples := []interface{}{"hello", 1.0, "X", 1, "2020-01-02T00:00:00Z"}

	// data
	input := [][]driver.Value{
		{"a", 1.0, "X", 1, time.Date(2020, 1, 2, 0.0, 0.0, 0.0, 0.0, time.UTC)},
		{"b", 2.0, "Y", 4, "junk"}}

	expected := [][]interface{}{
		{"a", 1.0, "X", 1, time.Date(2020, 1, 2, 0.0, 0.0, 0.0, 0.0, time.UTC)},
		{"b", -1.0, "Y", 4, dtMiss}}
	expectedV := []chutils.Valid{{chutils.VPass, chutils.VPass, chutils.VPass, chutils.VPass, chutils.VPass},
		{chutils.VPass, chutils.VValueFail, chutils.VPass, chutils.VPass, chutils.VTypeFail}}

	// build columns
	cols := make([]*sqlmock.Column, 0)
	for j := 0; j < len(resBases); j++ {
		tmp := sqlmock.NewColumn(resNames[j]).OfType(coltype[j], examples[j])
		cols = append(cols, tmp)
	}

	// build rows
	rows := sqlmock.NewRowsWithColumnDefinition(cols...)
	for j := 0; j < len(expected); j++ {
		rows = rows.AddRow(input[j]...)
	}
	// query issued by rdr.Init
	mock.ExpectQuery(`^SELECT \* FROM \(SELECT \* FROM bbb\) LIMIT 1`).WillReturnRows(rows)

	rdr := NewReader("SELECT * FROM bbb", con)
	if err := rdr.Init(); err != nil {
		t.Errorf("incorrect query")
	}
	// put in some bounds and missing values for these fields
	_, fd, err := rdr.TableSpec().Get("edate")
	fd.Missing = dtMiss
	_, fd, err = rdr.TableSpec().Get("bflt")
	fd.Missing = -1.0
	fd.Legal.HighLimit = 1.5
	fd.Legal.LowLimit = 0.0

	mock.ExpectQuery(`^SELECT \* FROM bbb`).WillReturnRows(rows)

	for r := 0; r < len(expected); r++ {
		data, v, err := rdr.Read(1, true)
		if err != nil {
			t.Errorf("Query failed")
		}
		assert.ElementsMatch(t, data[0], expected[r])
		assert.Equal(t, v[0], expectedV[r])
	}

	// re-build rows
	rows = sqlmock.NewRowsWithColumnDefinition(cols...)
	for j := 0; j < len(expected); j++ {
		rows = rows.AddRow(input[j]...)
	}

	mock.ExpectQuery(`^SELECT \* FROM bbb`).WillReturnRows(rows)
	rdr.Reset()

	data, _, err := rdr.Read(2, false)
	for r := 0; r < len(expected); r++ {
		for c := 0; c < len(data[r]); c++ {
			s := fmt.Sprintf("%v", input[r][c])
			d := data[r][c].(string)
			_, fd, _ := rdr.TableSpec().Get(resNames[c])
			// strip time out of date
			if fd.ChSpec.Base == chutils.ChDate && len(d) > 10 {
				d = d[0:10]
				s = s[0:10]
			}
			if d != s {
				t.Errorf("string read expected %s got %s", s, d)
			}
		}
	}

}

func TestReader_Seek(t *testing.T) {
	var db *sql.DB
	var mock sqlmock.Sqlmock
	var err error = nil
	con := &chutils.Connect{"", "", "", "", db}
	con.DB, mock, err = sqlmock.New()
	if err != nil {
		return
	}
	// result names & types
	resNames := []string{"a", "b"}
	resBases := []chutils.ChType{chutils.ChInt, chutils.ChString}

	// for sqlmock
	coltype := []string{"Int16", "String"}
	examples := []interface{}{1, "Hello"}

	// data
	input := [][]driver.Value{
		{1, "a"}, {2, "b"}, {3, "c"}, {4, "d"}, {5, "e"}}
	seekTo := []int{4, 2, 1, 10}
	expected := []interface{}{4, 2, 1, "ERR"}

	// build columns
	cols := make([]*sqlmock.Column, 0)
	for j := 0; j < len(resBases); j++ {
		tmp := sqlmock.NewColumn(resNames[j]).OfType(coltype[j], examples[j])
		cols = append(cols, tmp)
	}

	// build rows
	rows := sqlmock.NewRowsWithColumnDefinition(cols...)
	for j := 0; j < len(expected); j++ {
		rows = rows.AddRow(input[j]...)
	}
	// query issued by rdr.Init
	mock.ExpectQuery(`^SELECT \* FROM \(SELECT \* FROM bbb\) LIMIT 1`).WillReturnRows(rows)
	rdr := NewReader("SELECT * FROM bbb", con)
	if err := rdr.Init(); err != nil {
		t.Errorf("incorrect query")
	}
	for ind := 0; ind < len(seekTo); ind++ {
		rows := sqlmock.NewRowsWithColumnDefinition(cols...)
		for j := 0; j < len(expected); j++ {
			rows = rows.AddRow(input[j]...)
		}
		mock.ExpectQuery(`^SELECT \* FROM bbb`).WillReturnRows(rows)
		rdr.Seek(seekTo[ind])
		data, _, e := rdr.Read(1, true)
		if e != nil {
			if expected[ind] != "ERR" {
				t.Errorf("unexpected read error seek to %d", seekTo[ind])
			}
			break
		}
		assert.Equal(t, data[0][0], expected[ind])
	}
}

func TestWriter_Insert(t *testing.T) {
	var db *sql.DB
	var mock sqlmock.Sqlmock
	var err error = nil
	con := &chutils.Connect{"", "", "", "", db}
	con.DB, mock, err = sqlmock.New()
	if err != nil {
		return
	}
	_ = mock
	table := "aaa"
	wrtr := NewWriter(table, con)
	input := []string{"1, ab", "2, hello"}
	for ind := 0; ind < len(input); ind++ {
		if _, e := wrtr.Write([]byte(input[ind])); e != nil {
			t.Errorf("unexpected error writing")
		}
	}

	qry := fmt.Sprintf("^INSERT INTO %s VALUES", table)
	for ind := 0; ind < len(input); ind++ {
		qry += `\(` + input[ind] + `\)`
		if ind < len(input)-1 {
			qry += ","
		}
	}
	mock.ExpectExec(qry).WillReturnResult(driver.ResultNoRows)
	if e := wrtr.Insert(); e != nil {
		t.Errorf("unexpected Insert error")
	}
}
