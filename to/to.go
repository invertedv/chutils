// Package to provides simple methods to write chutils.Input data to a CSV or ClickHouse
package to

import (
	"fmt"
	"os"
	"strings"

	"github.com/invertedv/chutils"
	f "github.com/invertedv/chutils/file"
	s "github.com/invertedv/chutils/sql"
)

// ToCSV writes the rdr data out to the CSV toFile. The CSV includes a header row.
func ToCSV(rdr chutils.Input, outCSV string, separator, eol, quote rune) error {
	handle, err := os.Create(outCSV)
	if err != nil {
		return err
	}
	defer func() { _ = handle.Close() }()

	if _, e := handle.WriteString(fmt.Sprintf("%s\n", strings.Join(rdr.TableSpec().FieldList(), ","))); e != nil {
		return e
	}

	wtr := f.NewWriter(handle, outCSV, nil, separator, eol, quote, "")

	// after = -1 means it will not also write to ClickHouse
	if e := chutils.Export(rdr, wtr, -1, false); e != nil {
		return e
	}

	return rdr.Reset()
}

// ToTable writes the rdr data out to a ClickHouse table
func ToTable(rdr chutils.Input, outTable string, after int, conn *chutils.Connect) error {
	wtr := s.NewWriter(outTable, conn)
	if e := rdr.TableSpec().Create(conn, outTable); e != nil {
		return e
	}

	if e := chutils.Export(rdr, wtr, after, false); e != nil {
		return e
	}

	return rdr.Reset()
}

// TableToCSV creates a CSV from a ClickHouse table with a header row.
func TableToCSV(table, outCSV string, separator, eol, quote rune, conn *chutils.Connect) error {
	qry := fmt.Sprintf("SELECT * FROM %s", table)
	rdr := s.NewReader(qry, conn)

	if e := rdr.Init("", chutils.MergeTree); e != nil {
		return e
	}

	return ToCSV(rdr, outCSV, separator, eol, quote)
}

// CSVToTable moves a CSV to a ClickHouse table.
func CSVToTable(inCSV, outTable string, separator, eol, quote rune, after int, conn *chutils.Connect) error {
	const (
		width = 0
		skip  = 1
		tol   = 0.95
	)

	handle, e := os.Open(inCSV)
	if e != nil {
		return e
	}

	rdr := f.NewReader(inCSV, separator, eol, quote, width, skip, 0, handle, 0)
	if err := rdr.Init("", chutils.MergeTree); err != nil {
		return err
	}

	if err := rdr.TableSpec().Impute(rdr, 0, tol); err != nil {
		return err
	}

	return ToTable(rdr, outTable, after, conn)
}
