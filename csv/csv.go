package csv

//TODO: correctly handle opens/closes

import (
	"encoding/csv"
	"fmt"
	"github.com/invertedv/chutils"
	"io"
	"log"
	"math"
	"os"
	"time"
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
	rdr      *csv.Reader
	filename string
	// fileHandle to the csv
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
		TableSpec:  &chutils.TableDef{},
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

func (csvr *Reader) Init() (err error) {
	if csvr.RowsRead != 0 {
		return &chutils.ReadError{"Cannot call BuildTableD after lines have been read"}
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

	return
}

// BuildTableD builds the TableDef from a CSV csv
// rowsToExamine is the # of rows of csv to examine in building FieldDefs (e.g. Max, Min, Levels)
// tol is a fraction to determine the type.  If > tol rows are of type X, then this type is assigned
func (csvr *Reader) Build(rowsToExamine int, tol float64, fuzz int) (err error) {
	// countType keeps track of the field values as the csv is read
	type countType struct {
		floats int
		ints   int
		legal  *chutils.LegalValues
	}
	counts := make([]*countType, 0)
	for ind := 0; ind < len(csvr.TableSpec.FieldDefs); ind++ {
		ct := &countType{legal: chutils.NewLegalValues()}
		counts = append(counts, ct)
	}
	numFields := len(csvr.TableSpec.FieldDefs)
	// now look at RowsToExamine rows to see what types we have
	rowCount := 1
	for ; ; rowCount++ {
		row, err := csvr.rdr.Read()
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			err = &chutils.ReadError{Err: fmt.Sprintf("Error reading csv, row: %v", rowCount)}
			return err
		}
		for ind := 0; ind < len(row); ind++ {
			fval := row[ind]
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
		// only impute type if user has not specified it
		if csvr.TableSpec.FieldDefs[ind].ChSpec.Base == chutils.Unknown {
			switch {
			case counts[ind].ints >= thresh:
				csvr.TableSpec.FieldDefs[ind].ChSpec.Base = chutils.Int
				csvr.TableSpec.FieldDefs[ind].ChSpec.Length = 64
				csvr.TableSpec.FieldDefs[ind].Missing = math.MaxInt32
				// TODO: zero out Levels
				csvr.TableSpec.FieldDefs[ind].Legal = counts[ind].legal
				csvr.TableSpec.FieldDefs[ind].Legal.Levels = nil
			case counts[ind].floats >= thresh:
				csvr.TableSpec.FieldDefs[ind].ChSpec.Base = chutils.Float
				csvr.TableSpec.FieldDefs[ind].ChSpec.Length = 64
				csvr.TableSpec.FieldDefs[ind].Missing = math.MaxFloat32
				csvr.TableSpec.FieldDefs[ind].Legal = counts[ind].legal
				csvr.TableSpec.FieldDefs[ind].Legal.Levels = nil
			default:
				csvr.TableSpec.FieldDefs[ind].ChSpec.Base = chutils.String
				csvr.TableSpec.FieldDefs[ind].Missing = "Missing"
				csvr.TableSpec.FieldDefs[ind].Legal.Levels = counts[ind].legal.Levels
				csvr.TableSpec.FieldDefs[ind].Legal.LowLimit = nil
				csvr.TableSpec.FieldDefs[ind].Legal.HighLimit = nil
			}
		}
		if fuzz > 0 && (csvr.TableSpec.FieldDefs[ind].ChSpec.Base == chutils.String ||
			csvr.TableSpec.FieldDefs[ind].ChSpec.Base == chutils.FixedString) {
			// drop any with counts below fuzz
			for k, f := range *csvr.TableSpec.FieldDefs[ind].Legal.Levels {
				// TODO: change fuzz to float??
				if f <= fuzz {
					delete(*csvr.TableSpec.FieldDefs[ind].Legal.Levels, k)
				}
			}
		}
	}

	csvr.Reset()

	return nil
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

func Write(csvr *Reader) (err error) {
	csvr.Reset()
	//	csvr.Skip = 0
	fmt.Println("start reading", time.Now())
	data, err := csvr.Read(0)
	err = os.Remove("/home/will/tmp/try.csv")
	f, err := os.Create("/home/will/tmp/try.csv")
	if err != nil {
		log.Fatalln(err)
	}
	f.Close()
	fmt.Println("start writing", time.Now())
	file, err := os.OpenFile("/home/will/tmp/try.csv", os.O_RDWR, 0644)
	defer file.Close()
	if err != nil {
		log.Fatalln(err)
	}
	for r := 0; r < len(data); r++ {
		for c := 0; c < len(data[r]); c++ {
			char := ","
			if c == len(data[r])-1 {
				char = "\n"
			}
			switch v := data[r][c].(type) {
			case string:
				file.WriteString(fmt.Sprintf("'%s'%s", v, char))
			default:
				file.WriteString(fmt.Sprintf("%v%s", v, char))

			}
		}
	}
	fmt.Println("done writing", time.Now())

	return nil
}

// TODO: make enum of format types CSV, TabSeparated, CSVWithNames
