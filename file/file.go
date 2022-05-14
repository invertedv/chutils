// Package file implements Input/Output for text files.
// Text files can
//   - Be delimited or fixed-width
//   - Have header rows or not
//
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

// Reader implements chutils.Input interface.
type Reader struct {
	Skip      int               // Skip is the # of rows to skip in the file
	RowsRead  int               // RowsRead is current count of rows read from the file (includes header)
	MaxRead   int               // MaxRead is the maximum number of rows to read
	tableSpec *chutils.TableDef // TableSpec is the chutils.TableDef representing the fields in the source.  Can be supplied or derived from the file
	Width     int               // Width is the line width for flat files
	Quote     rune              // Quote is the optional quote around strings that contain the Separator
	eol       rune              // EOL is the end of line character
	separator rune              // Separator between fields in the file
	rdr       *bufio.Reader     // rdr is encoding/file package reader that reads from rws
	filename  string            // filename is source we are reading from
	rws       io.ReadSeekCloser // rws is the interface to source of data
	bufSize   int               // bufSize is the size of the bufio read buffer
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
		tableSpec: &chutils.TableDef{},
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

// TableSpec returns the TableDef
func (rdr *Reader) TableSpec() *chutils.TableDef {
	return rdr.tableSpec
}

// SetTableSpec sets Reader.tablespec.  Needed if tablespec is not created by Reader.TableSpec().Impute().
func (rdr *Reader) SetTableSpec(ts *chutils.TableDef) {
	rdr.tableSpec = ts
}

// Separator returns field separator rune
func (rdr *Reader) Separator() rune {
	return rdr.separator
}

// EOL returns end-of-line rune
func (rdr *Reader) EOL() rune {
	return rdr.eol
}

// Name returns the name of the file being read
func (rdr *Reader) Name() string {
	return rdr.filename
}

// Close closes the underlying ReadWriteSeeker
func (rdr *Reader) Close() error {
	return rdr.rws.Close()
}

// Reset sets the file pointer to the start of the file
func (rdr *Reader) Reset() error {
	if _, e := rdr.rws.Seek(0, 0); e != nil {
		return e
	}
	rdr.rdr = bufio.NewReaderSize(rdr.rws, rdr.bufSize)
	rdr.RowsRead = 0
	return nil
}

// Seek points the reader to lineNo line in the source data.
func (rdr *Reader) Seek(lineNo int) error {
	if _, e := rdr.rws.Seek(0, 0); e != nil {
		return e
	}
	rdr.rdr = bufio.NewReaderSize(rdr.rws, rdr.bufSize) // bufio.NewReader(csvr.rws)
	rdr.RowsRead = 0
	for ind := 0; ind < lineNo-1+rdr.Skip; ind++ {
		rdr.RowsRead++
		if _, err := rdr.rdr.ReadString(byte(rdr.EOL())); err != nil {
			return chutils.Wrapper(chutils.ErrSeek, fmt.Sprintf("line %d", ind))
		}
	}
	return nil
}

// CountLines returns the number of rows in the source data.  This does not include any header rows.
func (rdr *Reader) CountLines() (numLines int, err error) {
	if _, err = rdr.rws.Seek(0, 0); err != nil {
		return
	}
	rdr.rdr = bufio.NewReaderSize(rdr.rws, rdr.bufSize)
	defer func() { err = rdr.Reset() }()

	numLines = 0
	for e := error(nil); e != io.EOF; {
		if _, e = rdr.rdr.ReadString(byte(rdr.EOL())); e != nil {
			if e != io.EOF {
				return 0, chutils.Wrapper(chutils.ErrInput, "CountLines Failed")
			}
			numLines -= rdr.Skip
			return
		}
		numLines++
	}
	return
}

// Init initialize FieldDefs slice Reader.TableSpec() from header row of input.
// It does not set any of the field types.
func (rdr *Reader) Init(key string, engine chutils.EngineType) error {
	if rdr.RowsRead != 0 {
		if e := rdr.Reset(); e != nil {
			return e
		}
	}
	if rdr.Skip == 0 {
		return chutils.Wrapper(chutils.ErrInput, "Skip = 0 but need a header row")
	}
	row, err := rdr.getLine()
	rdr.RowsRead++
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
			Legal:       chutils.NewLegalValues(),
		}
		fds[ind] = fd
	}
	//	rdr.tableSpec.FieldDefs = fds
	rdr.tableSpec = chutils.NewTableDef(key, engine, fds)
	return nil
}

// GetLine returns the next line from Reader as a slice of strings
func (rdr *Reader) getLine() (line []string, err error) {
	err = nil
	if rdr.Width == 0 {
		var l string
		if l, err = rdr.rdr.ReadString(byte(rdr.EOL())); err != nil {
			// Do not declare EOF until l is empty
			if err != io.EOF || (l == "") {
				return nil, err
			}
			// hit EOF without an EOL
			l += "\n"
			err = nil
		}
		// No quote string, so just split on Separator.
		if rdr.Quote == 0 {
			// drop EOL
			l = strings.Replace(l, string(rdr.EOL()), "", 1)
			line = strings.Split(l, string(rdr.Separator()))
			// remove leading/trailing blanks
			for ind, l := range line {
				line[ind] = strings.Trim(l, " ")
			}
			return
		}
		// The file is quoted, so scan and don't count any Separator that occurs between the quotes.
		haveQuote := false
		f := make([]int32, len(l)) // temp slice where we put characters until we see a separator or EOL
		ind := 0
		for _, ch := range l {
			switch ch {
			case rdr.EOL():
				line = append(line, strings.Trim(string(f[0:ind]), " "))
			case rdr.Quote:
				haveQuote = !haveQuote
			case rdr.Separator():
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
	l := make([]byte, rdr.Width)
	if _, err = io.ReadFull(rdr.rdr, l); err != nil {
		if err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, err
	}
	lstr := string(l)
	if len(lstr) != rdr.Width {
		return nil, chutils.Wrapper(chutils.ErrFields, fmt.Sprintf("line is wrong width: %s", l))
	}
	line = make([]string, len(rdr.TableSpec().FieldDefs))
	start := 0
	for ind := 0; ind < len(line); ind++ {
		w := rdr.TableSpec().FieldDefs[ind].Width
		line[ind] = lstr[start : start+w]
		start += w
	}
	return
}

// Read reads nTarget rows.  If nTarget == 0, the entire file is read.
//
// If validation == true:
//   - The data is validated according to the rules in rdr.TableSpec.
//   - The results are returned as the slice valid.
//   - data is returned with the fields appropriately typed.
//
// If validation == false:
//   - The data is not validated.
//   - The return slice valid is nil
//   - The fields are returned as strings.
// err returns io.EOF at end of file
func (rdr *Reader) Read(nTarget int, validate bool) (data []chutils.Row, valid []chutils.Valid, err error) {

	if rdr.TableSpec().FieldDefs == nil {
		return nil, nil, chutils.Wrapper(chutils.ErrFields, "must define TableSpec")
	}
	var csvrow []string
	valid = nil

	if rdr.RowsRead == 0 && rdr.Skip > 0 {
		for i := 0; i < rdr.Skip; i++ {
			if _, err = rdr.getLine(); err != nil {
				return nil, nil, chutils.Wrapper(chutils.ErrInput, fmt.Sprintf("failed at row %d", i))
			}
		}
	}
	numFields := len(rdr.TableSpec().FieldDefs)

	data = make([]chutils.Row, 0)
	for rowCount := 1; ; rowCount++ {
		if csvrow, err = rdr.getLine(); err == io.EOF {
			return
		}
		if err != nil {
			err = chutils.Wrapper(chutils.ErrInput, fmt.Sprintf("read error at %d", rdr.RowsRead+1))
			return
		}
		if have, need := len(csvrow), len(rdr.TableSpec().FieldDefs); have != need {
			err = chutils.Wrapper(chutils.ErrFieldCount,
				fmt.Sprintf("at row %d, need %d fields but got %d", rdr.RowsRead+1, need, have))
			return
		}
		outrow := make(chutils.Row, 0)
		for j := 0; j < numFields; j++ {
			outrow = append(outrow, csvrow[j])
		}

		if validate {
			vrow := make(chutils.Valid, numFields)
			for j := 0; j < numFields; j++ {
				val, stat := rdr.TableSpec().FieldDefs[j].Validator(outrow[j])
				outrow[j] = val
				vrow[j] = stat
			}
			valid = append(valid, vrow)
		}
		data = append(data, outrow)
		rdr.RowsRead++
		if rdr.MaxRead > 0 && rdr.RowsRead > rdr.MaxRead {
			err = io.EOF
			return
		}
		if rowCount == nTarget && nTarget > 0 {
			return
		}
	}
}

// Rdrs generates slices of Readers of len nRdrs. The data represented by rdr0 is equally divided
// amongst the Readers in the slice.
func Rdrs(rdr0 *Reader, nRdrs int) (r []chutils.Input, err error) {

	r = nil
	nObs, err := rdr0.CountLines()
	if err != nil {
		return
	}
	if nRdrs < 1 {
		return nil, chutils.Wrapper(chutils.ErrInput, "must have >= 1 reader")
	}
	nper := nObs / nRdrs
	start := 1
	for ind := 0; ind < nRdrs; ind++ {
		var fh *os.File
		if fh, err = os.Open(rdr0.Name()); err != nil {
			return
		}
		np := start + nper - 1
		if ind == nRdrs-1 {
			np = 0
		}
		x := NewReader(rdr0.Name(), rdr0.Separator(), rdr0.EOL(), rdr0.Quote, rdr0.Width, rdr0.Skip, np, fh, rdr0.bufSize)
		x.tableSpec = rdr0.TableSpec()
		if err = x.Seek(start); err != nil {
			return
		}
		start += nper
		r = append(r, x)
	}
	return
}

// Writer implements chutils.Output.  Writer will accept any type that satisfies WriterCloser. Typically, this would
// be a file.
type Writer struct {
	io.WriteCloser
	Table     string           // Table is the ClickHouse table to Insert to
	name      string           // name of the file being written to
	separator rune             // separator is the field separator
	eol       rune             // eol is the end-of-line character
	conn      *chutils.Connect // Con is the ClickHouse connect info (needed for Insert)
}

// Insert inserts the file Writer.Name into ClickHouse table Writer.Table via the clickhouse-client program.
func (wtr *Writer) Insert() error {
	cmd := fmt.Sprintf("clickhouse-client --host=%s --user=%s", wtr.conn.Host, wtr.conn.User)
	if wtr.conn.Password != "" {
		cmd = fmt.Sprintf("%s --password=%s", cmd, wtr.conn.Password)
	}
	cmd = fmt.Sprintf("%s %s ", cmd, "")
	cmd = fmt.Sprintf("%s --format_csv_delimiter='%s'", cmd, string(wtr.Separator()))
	cmd = fmt.Sprintf("%s --query 'INSERT INTO %s FORMAT %s' < %s", cmd, wtr.Table, "CSV", wtr.Name())
	// running clickhouse-client as a command bc issuing the command itself chokes on --query element
	c := exec.Command("bash", "-c", cmd)
	return c.Run()
}

// Name returns the name of the file Writer points to.
func (wtr *Writer) Name() string {
	return wtr.name
}

// EOL returns the end-of-line rune
func (wtr *Writer) EOL() rune {
	return wtr.eol
}

// Separator returns the field separator rune
func (wtr *Writer) Separator() rune {
	return wtr.separator
}

// NewWriter creates a new Writer instance
func NewWriter(f io.WriteCloser, name string, con *chutils.Connect, separator rune, eol rune, table string) *Writer {
	return &Writer{f, table, name, separator, eol, con}
}

// Wrtrs creates a slice of Writers suitable for chutils.Concur.  The file names are chosen randomly.
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
