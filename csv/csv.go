package csv

//TODO: think about files w/o headers

import (
	"encoding/csv"
	"fmt"
	"github.com/invertedv/chutils"
	"io"
	"os"
)

// Reader is a reader compatible with chutils
type Reader struct {
	Separator  rune              // Separator between fields in the csv
	Skip       int               // Skip is the # of rows to skip in the csv
	RowsRead   int               // Rowsread is current count of rows read from the csv
	TableSpec  *chutils.TableDef // TableDef is the table def for the csv.  Can be supplied or derived from the csv
	rdr        *csv.Reader       // rdr is encoding/csv package reader
	filename   string            // file we are reading from
	fileHandle *os.File          // fileHandle to the csv
}

// NewReader initializes an instance of Reader
func NewReader(filename string, separator rune) (*Reader, error) {
	file, err := os.Open(filename)

	if err != nil {
		return nil, err
	}
	r := csv.NewReader(file)
	r.Comma = separator
	return &Reader{
		Separator:  separator,
		Skip:       0,
		RowsRead:   0,
		TableSpec:  &chutils.TableDef{},
		rdr:        r,
		filename:   filename,
		fileHandle: file,
	}, nil
}

func (csvr *Reader) Close() {
	csvr.fileHandle.Close()
}

// Reset sets the file pointer to the start of the file
func (csvr *Reader) Reset() {
	csvr.fileHandle.Seek(0, 0)
	csvr.rdr = csv.NewReader(csvr.fileHandle)
	csvr.rdr.Comma = csvr.Separator
	csvr.RowsRead = 0
	return
}

func (csvr *Reader) Init() (err error) {
	if csvr.RowsRead != 0 {
		return &chutils.InputError{"Cannot call BuildTableD after lines have been read"}
	}
	row, err := csvr.rdr.Read()
	if err != nil {
		return err
	}

	fds := make(map[int]*chutils.FieldDef)
	for ind, fn := range row {
		fd := &chutils.FieldDef{
			Name: fn,
			ChSpec: chutils.ChField{
				Base: chutils.Unknown},
			Description: "",
			Legal:       &chutils.LegalValues{},
		}
		fds[ind] = fd
	}
	csvr.TableSpec.FieldDefs = fds
	csvr.rdr.FieldsPerRecord = len(fds)
	csvr.Reset()

	return
}

// Read reads rows from the csv and do type conversion, validation
func (csvr *Reader) Read(numRow int, validate bool) (data []chutils.Row, err error) {
	var csvrow []string

	if csvr.RowsRead == 0 && csvr.Skip > 0 {
		for i := 0; i < csvr.Skip; i++ {
			_, err = csvr.rdr.Read()
			if err != nil {
				return
			}
		}
	}
	numFields := len(csvr.TableSpec.FieldDefs)
	csvr.rdr.FieldsPerRecord = numFields

	data = make([]chutils.Row, 0)
	for rowCount := 1; ; rowCount++ {
		csvrow, err = csvr.rdr.Read()
		if err == io.EOF {
			return
		}
		if err != nil {
			err = &chutils.InputError{Err: fmt.Sprintf("Error reading csv, row: %v", rowCount)}
			return
		}
		outrow := make(chutils.Row, 0)
		for j := 0; j < numFields; j++ {
			outrow = append(outrow, csvrow[j])
		}
		if validate {
			for j := 0; j < numFields; j++ {
				val, _ := csvr.TableSpec.FieldDefs[j].Validator(outrow[j], outrow, chutils.Pending)
				outrow[j] = val
			}
		}
		data = append(data, outrow)
		csvr.RowsRead++
		if rowCount == numRow && numRow > 0 {
			return
		}
	}
	return
}
