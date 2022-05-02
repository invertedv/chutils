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

func NewConnect() (con *chutils.Connect, err error) {
	var db *sql.DB
	var mock sqlmock.Sqlmock
	err = nil
	con = &chutils.Connect{"", "", "", "", db}
	con.DB, mock, err = sqlmock.New()
	if err != nil {
		return
	}
	_ = mock
	//	mock.ExpectPing()
	err = con.DB.Ping()
	return
}

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
	for j := range rdr.TableSpec.FieldDefs {
		actNames = append(actNames, rdr.TableSpec.FieldDefs[j].Name)
		actBases = append(actBases, rdr.TableSpec.FieldDefs[j].ChSpec.Base)
		fmt.Println(rdr.TableSpec.FieldDefs[j].Name)
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
		{"b", 2.0, "Y", 4, dtMiss}}

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

	_, fd, err := rdr.TableSpec.Get("edate")
	fd.Missing = dtMiss

	mock.ExpectQuery(`^SELECT \* FROM bbb`).WillReturnRows(rows)

	for r := 0; r < len(expected); r++ {
		data, err := rdr.Read(1, true)
		if err != nil {
			t.Errorf("Query failed")
		}
		assert.ElementsMatch(t, data[0], expected[r])
	}

}
