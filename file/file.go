// Package file handles creating readers for files
package file

import (
	"bufio"
	"fmt"
	"github.com/invertedv/chutils"
	"io"
	"math/rand"
	"os"
	"strings"
	"time"
)

// Reader is a Reader that satisfies chutils Input interface. It reads files.
type Reader struct {
	Separator  rune              // Separator between fields in the file
	Skip       int               // Skip is the # of rows to skip in the file
	RowsRead   int               // Rowsread is current count of rows read from the file
	MaxRead    int               // Max rows to read
	TableSpec  *chutils.TableDef // TableDef is the table def for the file.  Can be supplied or derived from the file
	EOL        rune              // EOL is the end of line character
	Width      int               // Line width for flat files
	Quote      rune              // Optional quote around strings that contain the Separator
	rdr        *bufio.Reader     // rdr is encoding/file package reader
	filename   string            // file we are reading from
	fileHandle io.ReadSeekCloser
}

//TODO: consider making this a io.ReadSeeker w/o close.
//TODO: consider do I need filename?

// NewReader initializes an instance of Reader
func NewReader(filename string, separator rune, eol rune, quote rune, width int, skip int, maxRead int, rws io.ReadSeekCloser) *Reader {
	r := bufio.NewReader(rws)

	return &Reader{
		Separator:  separator,
		Skip:       skip,
		RowsRead:   0,
		MaxRead:    maxRead,
		TableSpec:  &chutils.TableDef{},
		EOL:        eol,
		Width:      width,
		Quote:      quote,
		rdr:        r,
		filename:   filename,
		fileHandle: rws,
	}
}

func (csvr *Reader) Name() string {
	return csvr.filename
}

func (csvr *Reader) Close() error {
	return csvr.fileHandle.Close()
}

// Reset sets the file pointer to the start of the file
func (csvr *Reader) Reset() {
	_, _ = csvr.fileHandle.Seek(0, 0)
	csvr.rdr = bufio.NewReader(csvr.fileHandle)
	csvr.RowsRead = 0
	return
}

func (csvr *Reader) Seek(lineNo int) error {

	_, _ = csvr.fileHandle.Seek(0, 0)
	csvr.rdr = bufio.NewReader(csvr.fileHandle)
	csvr.RowsRead = 0
	for ind := 0; ind < lineNo-1+csvr.Skip; ind++ {
		if _, err := csvr.rdr.ReadString(byte(csvr.EOL)); err != nil {
			return chutils.NewChErr(chutils.ErrSeek, lineNo)
		}
	}
	return nil
}

func (csvr *Reader) CountLines() (numLines int, err error) {
	_, _ = csvr.fileHandle.Seek(0, 0)
	csvr.rdr = bufio.NewReader(csvr.fileHandle)
	defer csvr.Reset()

	numLines = 0
	err = nil
	for e := error(nil); e != io.EOF; {
		if _, e = csvr.rdr.ReadString(byte(csvr.EOL)); e != nil {
			if e != io.EOF {
				return 0, chutils.NewChErr(chutils.ErrInput, "CountLines")
			}
			numLines -= csvr.Skip
			return
		}
		numLines++
	}
	return
}

// Init initialize FieldDefs slice of TableDef from header row of input
func (csvr *Reader) Init() error {
	if csvr.RowsRead != 0 {
		csvr.Reset()
	}
	row, err := csvr.GetLine()
	if err != nil {
		return chutils.NewChErr(chutils.ErrInput, 0)
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
	return nil
}

// GetLine returns the next line from Reader and parses it into fields.
func (csvr *Reader) GetLine() (line []string, err error) {
	err = nil
	if csvr.Width == 0 {
		var l string
		if l, err = csvr.rdr.ReadString(byte(csvr.EOL)); err != nil {
			return nil, err
		}
		// No quote string, so just split on Separator.
		if csvr.Quote == 0 {
			line = strings.Split(l, string(csvr.Separator))
			return
		}
		// The file is quoted, so scan and don't count any Separator that occurs between the quotes.
		haveQuote := false
		f := make([]int32, len(l))
		ind := 0
		for _, ch := range l {
			switch ch {
			case csvr.EOL:
				line = append(line, string(f[0:ind]))
			case csvr.Quote:
				haveQuote = !haveQuote
			case csvr.Separator:
				if haveQuote {
					f[ind] = ch
					ind++
				} else {
					line = append(line, string(f[0:ind]))
					ind = 0
				}
			default:
				f[ind] = ch
				ind++
			}
		}
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
		return nil, chutils.NewChErr(chutils.ErrFields, "line is wrong width")
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

// Read reads numRow rows from, does type conversion and validation (validate==true)
func (csvr *Reader) Read(numRow int, validate bool) (data []chutils.Row, err error) {
	var csvrow []string

	if csvr.RowsRead == 0 && csvr.Skip > 0 {
		for i := 0; i < csvr.Skip; i++ {
			if _, err = csvr.GetLine(); err != nil {
				return nil, chutils.NewChErr(chutils.ErrInput, i)
			}
		}
	}
	numFields := len(csvr.TableSpec.FieldDefs)
	//	csvr.rdr.FieldsPerRecord = numFields

	data = make([]chutils.Row, 0)
	for rowCount := 1; ; rowCount++ {
		if csvrow, err = csvr.GetLine(); err == io.EOF {
			return
		}
		//		if err == io.EOF {
		//			return
		//		}
		if have, need := len(csvrow), len(csvr.TableSpec.FieldDefs); have != need {
			err = chutils.NewChErr(chutils.ErrFieldCount, need, have)
		}
		if err != nil {
			err = chutils.NewChErr(chutils.ErrInput, rowCount)
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
		if csvr.MaxRead > 0 && csvr.RowsRead > csvr.MaxRead {
			fmt.Println("Rows read", csvr.RowsRead)
			err = io.EOF
			return
		}
		if rowCount == numRow && numRow > 0 {
			return
		}
	}
}

// Divvy generates slices of len nChunks Input/Output by slicing up rdr0 data.  rdr0 is not part of the Input slice.
func Divvy(rdr0 *Reader, nChunks int, tmpDir string) (r []chutils.Input, w []chutils.Output, err error) {

	r = nil
	w = nil
	nObs, err := rdr0.CountLines()
	if err != nil {
		return
	}
	nper := nObs / nChunks
	start := 1
	for ind := 0; ind < nChunks; ind++ {
		var (
			fh *os.File
			a  *os.File
		)
		if fh, err = os.Open(rdr0.filename); err != nil {
			return
		}
		np := nper
		if ind == nChunks-1 {
			np = 0
		}
		x := NewReader(rdr0.filename, rdr0.Separator, rdr0.EOL, rdr0.Quote, rdr0.Width, rdr0.Skip, np, fh)
		x.TableSpec = rdr0.TableSpec
		if err = x.Seek(start); err != nil {
			return
		}
		start += nper
		r = append(r, x)

		rand.Seed(time.Now().UnixMicro())
		tmpFile := fmt.Sprintf("%s/tmp%d.csv", tmpDir, rand.Int31())
		if a, err = os.Create(tmpFile); err != nil {
			return
		}
		w = append(w, a)
	}
	return
}
