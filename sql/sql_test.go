package sql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/invertedv/chutils"
	"github.com/invertedv/chutils/file"
	//	_ "github.com/mailru/go-clickhouse/v2"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
	"time"
)

func TestReader_Init(t *testing.T) {
	var db *sql.DB
	var mock sqlmock.Sqlmock
	var err error
	con := &chutils.Connect{Host: "", User: "", Password: "", DB: db}
	con.DB, mock, err = sqlmock.New()
	if err != nil {
		return
	}

	// result names & types
	resNames := []string{"astr", "bflt", "cfstr", "dint", "edate"}
	resBases := []chutils.ChType{chutils.ChString, chutils.ChFloat, chutils.ChFixedString, chutils.ChInt, chutils.ChDate}

	// for sqlmock
	coltype := []string{"String", "Float64", "FixedString(1)", "Int32", "Date"}
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
	if err := rdr.Init("astr", chutils.MergeTree); err != nil {
		t.Errorf("unexpected fail in Init")
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
	var err error
	//	con := &chutils.Connect{"", "", "", "", db}
	con := &chutils.Connect{Host: "", User: "", Password: "", DB: db}
	con.DB, mock, err = sqlmock.New()
	if err != nil {
		return
	}
	// result names & types
	resNames := []string{"astr", "bflt", "cfstr", "dint", "edate"}
	resBases := []chutils.ChType{chutils.ChString, chutils.ChFloat, chutils.ChFixedString, chutils.ChInt, chutils.ChDate}

	// for sqlmock
	coltype := []string{"String", "Float64", "FixedString(1)", "Int32", "Date"}
	examples := []interface{}{"hello", 1.0, "X", 1, "2020-01-02T00:00:00Z"}

	// data
	input := [][]driver.Value{
		{"a", 1.0, "X", 1, time.Date(2020, 1, 2, 0.0, 0.0, 0.0, 0.0, time.UTC)},
		{"b", 2.0, "Y", 4, "junk"}}

	expected := [][]interface{}{
		{"a", 1.0, "X", int32(1), time.Date(2020, 1, 2, 0.0, 0.0, 0.0, 0.0, time.UTC)},
		{"b", -1.0, "Y", int32(4), dtMiss}}
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
	if err := rdr.Init("astr", chutils.MergeTree); err != nil {
		t.Errorf("incorrect query")
	}
	// put in some bounds and missing values for these fields

	var fd *chutils.FieldDef
	_, fd, err = rdr.TableSpec().Get("edate")
	if err != nil {
		t.Errorf("unexpected Get error")
	}
	fd.Missing = dtMiss
	_, fd, err = rdr.TableSpec().Get("bflt")
	if err != nil {
		t.Errorf("unexpected Get error")
	}
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
	if rdr.Reset() != nil {
		t.Errorf("unexpected error in Reset")
	}
}

func TestReader_Seek(t *testing.T) {
	var db *sql.DB
	var mock sqlmock.Sqlmock
	var err error
	//	con := &chutils.Connect{"", "", "", "", db}
	con := &chutils.Connect{Host: "", User: "", Password: "", DB: db}
	con.DB, mock, err = sqlmock.New()
	if err != nil {
		return
	}
	// result names & types
	resNames := []string{"a", "b"}
	resBases := []chutils.ChType{chutils.ChInt, chutils.ChString}

	// for sqlmock
	coltype := []string{"Int32", "String"}
	examples := []interface{}{1, "Hello"}

	// data
	input := [][]driver.Value{
		{1, "a"}, {2, "b"}, {3, "c"}, {4, "d"}, {5, "e"}}
	seekTo := []int{4, 2, 1, 10}
	expected := []interface{}{int32(4), int32(2), int32(1), chutils.ErrSeek}

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
	if err := rdr.Init("a", chutils.MergeTree); err != nil {
		t.Errorf("incorrect query")
	}
	for ind := 0; ind < len(seekTo); ind++ {
		rows := sqlmock.NewRowsWithColumnDefinition(cols...)
		for j := 0; j < len(expected); j++ {
			rows = rows.AddRow(input[j]...)
		}
		mock.ExpectQuery(`^SELECT \* FROM bbb`).WillReturnRows(rows)
		if e := rdr.Seek(seekTo[ind]); e != nil {
			if errors.Unwrap(e) != chutils.ErrSeek || expected[ind] != chutils.ErrSeek {
				t.Errorf("unexpected error in Seek")
			}
			continue
		}
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
	var err error
	//	con := &chutils.Connect{"", "", "", "", db}
	con := &chutils.Connect{Host: "", User: "", Password: "", DB: db}
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

// This example reads from a file.Reader and writes to ClickHouse using a sql.Writer
func ExampleWriter_Write() {
	/*
		/home/test/data/zip_data.csv:
		id,zip,value
		1A34,90210,20.8
		1X88,43210,19.2
		1B23,77810,NA
		1r99,94043,100.4
		1x09,hello,9.9
	*/

	const inFile = "/home/will/tmp/zip_data.csv" // source data
	const table = "testing.values"               // ClickHouse destination table
	var con *chutils.Connect
	con, err := chutils.NewConnect("127.0.0.1", "tester", "testGoNow", clickhouse.Settings{})
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if con.Close() != nil {
			log.Fatalln(err)
		}
	}()
	f, err := os.Open(inFile)
	if err != nil {
		log.Fatalln(err)
	}
	rdr := file.NewReader(inFile, ',', '\n', '"', 0, 1, 0, f, 50000)
	defer func() {
		if rdr.Close() != nil {
			log.Fatalln(err)
		}
	}()
	if e := rdr.Init("zip", chutils.MergeTree); e != nil {
		log.Fatalln(err)
	}
	if e := rdr.TableSpec().Impute(rdr, 0, .95); e != nil {
		log.Fatalln(e)
	}

	// Specify zip as FixedString(5) with a missing value of 00000
	_, fd, err := rdr.TableSpec().Get("zip")
	if err != nil {
		log.Fatalln(err)
	}
	// zip will impute to int if we don't make this change
	fd.ChSpec.Base = chutils.ChFixedString
	fd.ChSpec.Length = 5
	fd.Missing = "00000"
	legal := []string{"90210", "43210", "77810", "94043"}
	fd.Legal.Levels = legal

	// Specify value as having a range of [0,30] with a missing value of -1.0
	_, fd, err = rdr.TableSpec().Get("value")
	if err != nil {
		log.Fatalln(err)
	}
	fd.Legal.HighLimit = 30.0
	fd.Legal.LowLimit = 0.0
	fd.Missing = -1.0

	rdr.TableSpec().Engine = chutils.MergeTree
	rdr.TableSpec().Key = "id"
	if err := rdr.TableSpec().Create(con, table); err != nil {
		log.Fatalln(err)
	}

	wrtr := NewWriter(table, con)
	if err := chutils.Export(rdr, wrtr, 0); err != nil {
		log.Fatalln(err)
	}
	qry := fmt.Sprintf("SELECT * FROM %s", table)
	res, err := con.Query(qry)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if res.Close() != nil {
			log.Fatalln(err)
		}
	}()
	for res.Next() {
		var (
			id    string
			zip   string
			value float64
		)
		if res.Scan(&id, &zip, &value) != nil {
			log.Fatalln(err)
		}
		fmt.Println(id, zip, value)
	}
	// Output:
	//1A34 90210 20.8
	//1B23 77810 -1
	//1X88 43210 19.2
	//1r99 94043 -1
	//1x09 00000 9.9

}
