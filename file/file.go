// file package handles creating readers for files
package file

import (
	"bufio"
	"fmt"
	"github.com/invertedv/chutils"
	"io"
	"os"
	"strings"
)

// Reader is a reader compatible with chutils
type Reader struct {
	Separator  string            // Separator between fields in the file
	Skip       int               // Skip is the # of rows to skip in the file
	RowsRead   int               // Rowsread is current count of rows read from the file
	TableSpec  *chutils.TableDef // TableDef is the table def for the file.  Can be supplied or derived from the file
	EOL        byte              // EOL is the end of line character
	Width      int               // Line width for flat files
	rdr        *bufio.Reader     // rdr is encoding/file package reader
	filename   string            // file we are reading from
	fileHandle *os.File          // fileHandle to the file

}

// NewReader initializes an instance of Reader
func NewReader(filename string, separator string, eol byte, width int) (*Reader, error) {
	file, err := os.Open(filename)

	if err != nil {
		return nil, err
	}
	r := bufio.NewReader(file)
	return &Reader{
		Separator:  separator,
		Skip:       0,
		RowsRead:   0,
		TableSpec:  &chutils.TableDef{},
		EOL:        eol,
		Width:      width,
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
	csvr.rdr = bufio.NewReader(csvr.fileHandle)
	//	csvr.rdr = file.NewReader(csvr.fileHandle)
	//	csvr.rdr.Comma = csvr.Separator
	csvr.RowsRead = 0
	return
}

func (csvr *Reader) Seek(lineNo int) (err error) {

	csvr.fileHandle.Seek(0, 0)
	csvr.rdr = bufio.NewReader(csvr.fileHandle)
	for ind := 0; ind < lineNo-1+csvr.Skip; ind++ {
		if _, err = csvr.rdr.ReadString(csvr.EOL); err != nil {
			return
		}
	}
	return
}

func (csvr *Reader) Init() (err error) {
	if csvr.RowsRead != 0 {
		return &chutils.InputError{"Cannot call BuildTableD after lines have been read"}
	}
	row, err := csvr.GetLine()
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
	// don't think this is needed
	//	csvr.Reset()

	return
}

func (csvr *Reader) GetLine() (line []string, err error) {
	if csvr.Width == 0 {
		var l string
		if l, err = csvr.rdr.ReadString(csvr.EOL); err != nil {
			return make([]string, 0), err
		}
		line = strings.Split(l, csvr.Separator)
		return
	}
	// file has fixed-width structure (flat file)
	l := make([]byte, csvr.Width)
	if _, err = io.ReadFull(csvr.rdr, l); err != nil {
		if err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, err
	}
	lstr := string(l)
	if len(lstr) != csvr.Width {
		return make([]string, 0), &chutils.InputError{"Wrong width"}
	}
	line = make([]string, len(csvr.TableSpec.FieldDefs))
	start := 0
	for ind := 0; ind < len(line); ind++ {
		w := csvr.TableSpec.FieldDefs[ind].Width
		line[ind] = lstr[start : start+w]
		start += w
	}
	return
}

// Read reads rows from the file and do type conversion, validation
func (csvr *Reader) Read(numRow int, validate bool) (data []chutils.Row, err error) {
	var csvrow []string

	if csvr.RowsRead == 0 && csvr.Skip > 0 {
		for i := 0; i < csvr.Skip; i++ {
			_, err = csvr.GetLine()
			if err != nil {
				return
			}
		}
	}
	numFields := len(csvr.TableSpec.FieldDefs)
	//	csvr.rdr.FieldsPerRecord = numFields

	data = make([]chutils.Row, 0)
	for rowCount := 1; ; rowCount++ {
		csvrow, err = csvr.GetLine()
		if err == io.EOF {
			return
		}
		if have, need := len(csvrow), len(csvr.TableSpec.FieldDefs); have != need {
			errstr := fmt.Sprintf("Row %v has %v fields but need %v", csvr.RowsRead, have, need)
			err = &chutils.InputError{errstr}
		}
		if err != nil {
			err = &chutils.InputError{Err: fmt.Sprintf("Error reading file, row: %v", rowCount)}
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
