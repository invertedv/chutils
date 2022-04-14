package csv

//TODO: correctly handle opens/closes

import (
	"encoding/csv"
	"fmt"
	"github.com/invertedv/chutils"
	"io"
	"math"
	"os"
)

// Reader is a reader compatible with chutils
type Reader struct {
	// Separator between fields in the csv
	Separator rune
	// Skip is the # of rows to skip in the csv
	Skip int
	// Rowsread is current count of rows read from the csv
	RowsRead int
	// TableDef is the table def for the csv.  Can be supplied or derived from the csv
	TableSpec *chutils.TableDef
	// rdr is csv package reader
	rdr *csv.Reader
	// filename is the name of the csv being read
	filename   string
	fileHandle *os.File
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
		TableSpec:  nil,
		rdr:        r,
		filename:   filename,
		fileHandle: file,
	}, nil
}

func (csvr *Reader) Close() {
	csvr.fileHandle.Close()
}

func (csvr *Reader) Reset() {
	// Reset the csv to the beginning
	csvr.fileHandle.Seek(0, 0)
	csvr.rdr = csv.NewReader(csvr.fileHandle)
	csvr.rdr.Comma = csvr.Separator
	csvr.RowsRead = 0
	return
}

// BuildTableD builds the TableDef from a CSV csv
// rowsToExamine is the # of rows of csv to examine in building FieldDefs (e.g. Max, Min, Levels)
// tol is a fraction to determine the type.  If > tol rows are of type X, then this type is assigned
func (csvr *Reader) BuildTableDef(rowsToExamine int, tol float64) (td *chutils.TableDef, err error) {
	// countType keeps track of the field values as the csv is read
	type countType struct {
		floats int
		ints   int
		legal  chutils.LegalValues
	}

	if csvr.RowsRead != 0 {
		return nil, &chutils.ReadError{"Cannot call BuildTableD after lines have been read"}
	}
	row, err := csvr.rdr.Read()
	if err != nil {
		return nil, err
	}

	fds := make(map[int]*chutils.FieldDef)
	td = &chutils.TableDef{}
	counts := make([]*countType, 0)
	for ind, fn := range row {
		fd := &chutils.FieldDef{
			Name: fn,
			ChSpec: chutils.ChField{
				Base: chutils.Unknown},
			Description: "",
			Legal:       chutils.LegalValues{},
		}
		fds[ind] = fd
		ct := &countType{}
		counts = append(counts, ct)
	}
	numFields := len(fds)
	csvr.rdr.FieldsPerRecord = numFields

	// now look at RowsToExamine rows to see what types we have
	rowCount := 1
	for ; ; rowCount++ {
		row, err = csvr.rdr.Read()
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			err = &chutils.ReadError{Err: fmt.Sprintf("Error reading csv, row: %v", rowCount)}
			return nil, err
		}
		for ind := 0; ind < len(row); ind++ {
			fval := row[ind]
			//		for ind, fval := range row {
			if counts[ind].legal.Update(fval, "int") {
				counts[ind].ints++
			} else {
				if counts[ind].legal.Update(fval, "float") {
					counts[ind].floats++
				} else {
					counts[ind].legal.Update(fval, "string")
				}
			}
		}
		if rowCount == rowsToExamine && rowsToExamine > 0 {
			break
		}
	}

	// threshold to determine which type a field is (100*tol % agreement)
	thresh := int(math.Max(1.0, tol*float64(rowCount)))
	for ind := 0; ind < numFields; ind++ {
		switch {
		case counts[ind].ints > thresh:
			fds[ind].ChSpec.Base = chutils.Int
			fds[ind].ChSpec.Length = 64
			fds[ind].Missing = math.MaxInt32
			fds[ind].Legal = counts[ind].legal
		case counts[ind].floats > thresh:
			fds[ind].ChSpec.Base = chutils.Float
			fds[ind].ChSpec.Length = 64
			fds[ind].Missing = math.MaxFloat32
			fds[ind].Legal = counts[ind].legal
		default:
			fds[ind].ChSpec.Base = chutils.String
			fds[ind].Missing = "Missing"
			fds[ind].Legal = counts[ind].legal
		}
	}

	td.FieldDefs = fds
	csvr.Reset()

	return td, nil
}

// Read reads rows from the csv and do type conversion, validation
func (csvr *Reader) Read(numRow int) (data []chutils.Row, err error) {
	var csvrow []string
	//TODO: this is not always working
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
			err = nil
			return
		}
		if err != nil {
			err = &chutils.ReadError{Err: fmt.Sprintf("Error reading csv, row: %v", rowCount)}
			return
		}
		outrow := make(chutils.Row, 0)
		for j := 0; j < numFields; j++ {
			outrow = append(outrow, csvrow[j])
		}
		for j := 0; j < numFields; j++ {
			val, _ := csvr.TableSpec.FieldDefs[j].Validator(outrow[j], outrow, chutils.Pending)
			outrow[j] = val
		}
		data = append(data, outrow)
		csvr.RowsRead++
		if rowCount == numRow && numRow > 0 {
			return
		}
	}

	return
}
