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

// Csv writes the rdr data out to the CSV toFile
func Csv(rdr chutils.Input, toFile string) error {
	handle, err := os.Create(toFile)
	if err != nil {
		return err
	}
	defer func() { _ = handle.Close() }()

	if _, e := handle.WriteString(fmt.Sprintf("%s\n", strings.Join(rdr.TableSpec().FieldList(), ","))); e != nil {
		return e
	}

	wtr := f.NewWriter(handle, toFile, nil, ',', '\n', "")

	// after = -1 means it will not also write to ClickHouse
	if e := chutils.Export(rdr, wtr, -1, false); e != nil {
		return e
	}

	return rdr.Reset()
}

// Sql writes the rdr data out to a ClickHouse table
func Sql(rdr chutils.Input, outTable string, after int, conn *chutils.Connect) error {
	wtr := s.NewWriter(outTable, conn)
	if e := rdr.TableSpec().Create(conn, outTable); e != nil {
		return e
	}

	if e := chutils.Export(rdr, wtr, after, false); e != nil {
		return e
	}

	return rdr.Reset()
}
