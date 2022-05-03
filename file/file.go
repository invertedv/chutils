// Package file handles creating readers for files
package file

import (
	"bufio"
	"fmt"
	"github.com/invertedv/chutils"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"time"
)

// Reader is a Reader that satisfies chutils Input interface. It reads files.
type Reader struct {
	Skip      int               // Skip is the # of rows to skip in the file
	RowsRead  int               // RowsRead is current count of rows read from the file (includes header)
	MaxRead   int               // MaxRead is the maximum number of rows to read
	TableSpec *chutils.TableDef // TableSpec is the chutils.TableDef representing the fields in the source.  Can be supplied or derived from the file
	Width     int               // Width is the line width for flat files
	Quote     rune              // Quote is the optional quote around strings that contain the Separator
	eol       rune              // EOL is the end of line character
	separator rune              // Separator between fields in the file
	rdr       *bufio.Reader     // rdr is encoding/file package reader that reads from rws
	filename  string            // filename is source we are reading from
	rws       io.ReadSeekCloser // rws is the interface to source of data
	bufSize   int
}

// NewReader initializes an instance of Reader
func NewReader(filename string, separator rune, eol rune, quote rune, width int, skip int, maxRead int,
	rws io.ReadSeekCloser, bufSize int) *Reader {
	if bufSize == 0 {
		bufSize = 4096
	}
	r := bufio.NewReaderSize(rws, bufSize)

	return &Reader{
		Skip:      skip,
		RowsRead:  0,
		MaxRead:   maxRead,
		TableSpec: &chutils.TableDef{},
		Width:     width,
		Quote:     quote,
		eol:       eol,
		separator: separator,
		rdr:       r,
		filename:  filename,
		rws:       rws,
		bufSize:   bufSize,
	}
}

func (csvr *Reader) Separator() rune {
	return csvr.separator
}

func (csvr *Reader) EOL() rune {
	return csvr.eol
}

func (csvr *Reader) Name() string {
	return csvr.filename
}

func (csvr *Reader) Close() error {
	return csvr.rws.Close()
}

// Reset sets the file pointer to the start of the file
func (csvr *Reader) Reset() error {
	_, _ = csvr.rws.Seek(0, 0)
	csvr.rdr = bufio.NewReaderSize(csvr.rws, csvr.bufSize)
	csvr.RowsRead = 0
	return nil
}

func (csvr *Reader) Seek(lineNo int) error {
	_, _ = csvr.rws.Seek(0, 0)
	csvr.rdr = bufio.NewReaderSize(csvr.rws, csvr.bufSize) // bufio.NewReader(csvr.rws)
	csvr.RowsRead = 0
	for ind := 0; ind < lineNo-1+csvr.Skip; ind++ {
		csvr.RowsRead++
		if _, err := csvr.rdr.ReadString(byte(csvr.EOL())); err != nil {
			return chutils.Wrapper(chutils.ErrSeek, fmt.Sprintf("line %d", ind))
		}
	}
	return nil
}

func (csvr *Reader) CountLines() (numLines int, err error) {
	_, _ = csvr.rws.Seek(0, 0)
	csvr.rdr = bufio.NewReaderSize(csvr.rws, csvr.bufSize) // bufio.NewReader(csvr.rws)
	defer csvr.Reset()

	numLines = 0
	err = nil
	for e := error(nil); e != io.EOF; {
		if _, e = csvr.rdr.ReadString(byte(csvr.EOL())); e != nil {
			if e != io.EOF {
				return 0, chutils.Wrapper(chutils.ErrInput, "CountLines Failed")
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
	csvr.RowsRead++
	if err != nil {
		return chutils.Wrapper(chutils.ErrInput, "initial read failed")
	}

	fds := make(map[int]*chutils.FieldDef)
	for ind, fn := range row {
		fd := &chutils.FieldDef{
			Name: fn,
			ChSpec: chutils.ChField{
				Base: chutils.ChUnknown},
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
		if l, err = csvr.rdr.ReadString(byte(csvr.EOL())); err != nil {
			return nil, err
		}
		// No quote string, so just split on Separator.
		if csvr.Quote == 0 {
			// drop EOL
			l = strings.Replace(l, string(csvr.EOL()), "", 1)
			line = strings.Split(l, string(csvr.Separator()))
			// remove leading/trailing blanks
			for ind, l := range line {
				line[ind] = strings.Trim(l, " ")
			}
			return
		}
		// The file is quoted, so scan and don't count any Separator that occurs between the quotes.
		haveQuote := false
		f := make([]int32, len(l))
		ind := 0
		for _, ch := range l {
			switch ch {
			case csvr.EOL():
				line = append(line, strings.Trim(string(f[0:ind]), " "))
			case csvr.Quote:
				haveQuote = !haveQuote
			case csvr.Separator():
				if haveQuote {
					f[ind] = ch
					ind++
				} else {
					line = append(line, strings.Trim(string(f[0:ind]), " "))
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
		return nil, chutils.Wrapper(chutils.ErrFields, "line is wrong width")
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
				return nil, chutils.Wrapper(chutils.ErrInput, fmt.Sprintf("failed at row %d", i))
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
		if have, need := len(csvrow), len(csvr.TableSpec.FieldDefs); have != need {
			err = chutils.Wrapper(chutils.ErrFieldCount,
				fmt.Sprintf("at row %d, need %d fields but got %d", csvr.RowsRead+1, need, have))
			return
		}
		if err != nil {
			err = chutils.Wrapper(chutils.ErrInput, fmt.Sprintf("read error at %d", csvr.RowsRead+1))
			return
		}
		outrow := make(chutils.Row, 0)
		for j := 0; j < numFields; j++ {
			outrow = append(outrow, csvrow[j])
		}
		if validate {
			for j := 0; j < numFields; j++ {
				val, _ := csvr.TableSpec.FieldDefs[j].Validator(outrow[j], csvr.TableSpec, outrow, chutils.VPending)
				outrow[j] = val
			}
		}
		data = append(data, outrow)
		csvr.RowsRead++
		if csvr.MaxRead > 0 && csvr.RowsRead > csvr.MaxRead {
			err = io.EOF
			return
		}
		if rowCount == numRow && numRow > 0 {
			return
		}
	}
}

// Rdrs generates slices of len nChunks Input/Output by slicing up rdr0 data.  rdr0 is not part of the Input slice.
func Rdrs(rdr0 *Reader, nRdrs int) (r []chutils.Input, err error) {

	r = nil //make([]*Reader, 0)
	nObs, err := rdr0.CountLines()
	if err != nil {
		return
	}
	nper := nObs / nRdrs
	start := 1
	for ind := 0; ind < nRdrs; ind++ {
		var fh *os.File
		if fh, err = os.Open(rdr0.filename); err != nil {
			return
		}
		np := start + nper - 1
		if ind == nRdrs-1 {
			np = 0
		}
		x := NewReader(rdr0.filename, rdr0.Separator(), rdr0.EOL(), rdr0.Quote, rdr0.Width, rdr0.Skip, np, fh, rdr0.bufSize)
		x.TableSpec = rdr0.TableSpec
		if err = x.Seek(start); err != nil {
			return
		}
		start += nper
		r = append(r, x)
	}
	return
}

type Writer struct {
	io.WriteCloser
	name      string
	separator rune             // separator is the field separator
	eol       rune             // eol is the end-of-line character
	conn      *chutils.Connect // Con is the ClickHouse connect info (needed for Insert)
	Table     string           // Table is the table to load created file to (needed for Insert)
}

func (w *Writer) Insert() error {
	cmd := fmt.Sprintf("clickhouse-client --host=%s --user=%s", w.conn.Host, w.conn.User)
	if w.conn.Password != "" {
		cmd = fmt.Sprintf("%s --password=%s", cmd, w.conn.Password)
	}
	cmd = fmt.Sprintf("%s %s ", cmd, "")
	cmd = fmt.Sprintf("%s --format_csv_delimiter='%s'", cmd, string(w.Separator()))
	cmd = fmt.Sprintf("%s --query 'INSERT INTO %s FORMAT %s' < %s", cmd, w.Table, "CSV", w.Name())
	// running clickhouse-client as a command bc issuing the command itself chokes on --query element
	c := exec.Command("bash", "-c", cmd)
	err := c.Run()
	return err
}

func (w *Writer) Name() string {
	return w.name
}

func (w *Writer) EOL() rune {
	return w.eol
}

func (w *Writer) Separator() rune {
	return w.separator
}

func NewWriter(f io.WriteCloser, name string, con *chutils.Connect, separator rune, eol rune, table string) *Writer {
	return &Writer{f, name, separator, eol, con, table}
}

func Wrtrs(tmpDir string, nWrtr int, con *chutils.Connect, separator rune, eol rune, table string) (wrtrs []chutils.Output, err error) {
	var a *os.File

	wrtrs = nil
	rand.Seed(time.Now().UnixMicro())

	for ind := 0; ind < nWrtr; ind++ {
		tmpFile := fmt.Sprintf("%s/tmp%d.csv", tmpDir, rand.Int31())
		if a, err = os.Create(tmpFile); err != nil {
			return
		}
		w := NewWriter(a, tmpFile, con, separator, eol, table)
		wrtrs = append(wrtrs, w)
	}
	return
}
