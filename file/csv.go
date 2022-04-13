package file

import (
	"encoding/csv"
	"fmt"
	"github.com/invertedv/chutils"
	"io"
	"math"
	"os"
	"strconv"
)

// CSVReader is a reader compatible with chutils
type CsvReader struct {
	// Separator between fields in the file
	Separator rune
	// Skip is the # of rows to skip in the file
	Skip int
	// Rowsread is current count of rows read from the file
	RowsRead int
	// TableDef is the table def for the file.  Can be supplied or derived from the file
	TableDef *chutils.TableDef
	// rdr is csv package reader
	rdr *csv.Reader
	// filename is the name of the file being read
	filename string
}

// NewCsvReader initializes an instance of CsvReader
func NewCsvReader(filename string, separator rune) *CsvReader {
	file, err := os.Open(filename)
	if err != nil {
		return nil
	}
	r := csv.NewReader(file)
	r.Comma = separator
	return &CsvReader{
		Separator: separator,
		Skip:      0,
		RowsRead:  0,
		TableDef:  nil,
		rdr:       r,
		filename:  filename,
	}
}

// countType keeps track of the field values as the file is read
type countType struct {
	floats    int
	ints      int
	HighLimit chutils.Limits
	LowLimit  chutils.Limits
	Levels    map[string]int
}

// BuildTableD builds the TableDef from a CSV file
// rowsToExamine is the # of rows of csv to examine in building FieldDefs (e.g. Max, Min, Levels)
// tol is a fraction to determine the type.  If > tol rows are of type X, then this type is assigned
func (csvr *CsvReader) BuildTableDef(rowsToExamine int, tol float64) (td *chutils.TableDef, err error) {
	if csvr.RowsRead != 0 {
		return nil, &chutils.ReadError{"Cannot call BuildTableD after lines have been read"}
	}
	row, err := csvr.rdr.Read()
	if err != nil {
		return nil, err
	}

	fds := make(map[int]*chutils.FieldDef)
	td = &chutils.TableDef{}
	counts := make([]countType, 0)
	for ind, fn := range row {
		fd := &chutils.FieldDef{
			FieldName: fn,
			ChType: chutils.ChField{
				TypeBase: chutils.Unknown},
			Description: "",
			Levels:      nil,
			HighLimit:   chutils.Limits{},
			LowLimit:    chutils.Limits{},
		}
		fds[ind] = fd
		ct := countType{}
		ct.LowLimit.AsFloat = math.MaxFloat64
		ct.LowLimit.AsInt = math.MaxInt
		ct.HighLimit.AsFloat = -math.MaxFloat64
		ct.HighLimit.AsInt = -math.MaxInt

		ct.Levels = make(map[string]int)
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
			err = &chutils.ReadError{Err: fmt.Sprintf("Error reading file, row: %v", rowCount)}
			return nil, err
		}

		for ind, fval := range row {
			if v, err := strconv.ParseFloat(fval, 64); err == nil {
				counts[ind].floats++
				counts[ind].HighLimit.AsFloat = math.Max(v, float64(counts[ind].HighLimit.AsFloat))
				counts[ind].LowLimit.AsFloat = math.Min(v, float64(counts[ind].HighLimit.AsFloat))
			}
			if v, err := strconv.ParseInt(fval, 10, 64); err == nil {
				counts[ind].ints++
				if counts[ind].HighLimit.AsInt < v {
					counts[ind].HighLimit.AsInt = v
				}
				if counts[ind].LowLimit.AsInt > v {
					counts[ind].LowLimit.AsInt = v
				}
			}
			counts[ind].Levels[fval]++
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
			fds[ind].ChType.TypeBase = chutils.Int
			fds[ind].ChType.TypeLength = 64
			fds[ind].HighLimit.AsInt = counts[ind].HighLimit.AsInt
			fds[ind].LowLimit.AsInt = counts[ind].LowLimit.AsInt
			fds[ind].Missing = -1000
		case counts[ind].floats > thresh:
			fds[ind].ChType.TypeBase = chutils.Float
			fds[ind].ChType.TypeLength = 64
			fds[ind].HighLimit.AsFloat = counts[ind].HighLimit.AsFloat
			fds[ind].LowLimit.AsFloat = counts[ind].LowLimit.AsFloat
			fds[ind].Missing = -1000.0
		default:
			fds[ind].ChType.TypeBase = chutils.String
			fds[ind].Levels = counts[ind].Levels
			fds[ind].Missing = "Missing"
		}
	}

	td.FieldDefs = fds
	// Only way to reset the file for encoding/csv is to make a new reader
	f, err := os.Open(csvr.filename)
	if err != nil {
		return
	}
	csvr.rdr = csv.NewReader(f)
	csvr.rdr.Comma = csvr.Separator

	return td, nil
}

// Read reads rows from the csv and do type conversion, validation
func (csvr *CsvReader) Read(numRow int) (data []chutils.Row, err error) {
	var csvrow []string

	if csvr.RowsRead == 0 && csvr.Skip > 0 {
		for i := 0; i < csvr.Skip; i++ {
			_, err = csvr.rdr.Read()
			if err != nil {
				return
			}
		}
	}
	numFields := len(csvr.TableDef.FieldDefs)
	csvr.rdr.FieldsPerRecord = numFields

	data = make([]chutils.Row, 0)
	for rowCount := 1; ; rowCount++ {
		csvrow, err = csvr.rdr.Read()
		if err == io.EOF {
			err = nil
			return
		}
		if err != nil {
			err = &chutils.ReadError{Err: fmt.Sprintf("Error reading file, row: %v", rowCount)}
			return
		}
		outrow := make(chutils.Row, 0)
		for j := 0; j < numFields; j++ {
			outrow = append(outrow, csvrow[j])
		}
		for j := 0; j < numFields; j++ {
			val, _ := csvr.TableDef.FieldDefs[j].Validator(outrow[j], outrow, chutils.Pending)
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
