package ram

import (
	"fmt"
	"io"

	"github.com/invertedv/chutils"
)

// Reader struct satisfies chutils.Input.  The Data/Validate slices are the same as those returned by the Read method
// of chutils.Input.  This can be useful when one wants to read data, alter it in some way, and then re-read it or
// write it out.
type Reader struct {
	Data      []chutils.Row     // Data
	tableSpec *chutils.TableDef // TableDef of the data
	onRow     int               // onRow is current count of rows read from the file (includes header)
}

// NewReader creates a new Reader instance
func NewReader(data []chutils.Row, tableSpec *chutils.TableDef) (ramData *Reader, err error) {
	if data == nil {
		return nil, fmt.Errorf("data cannot be nil in NewAnyData")
	}

	dataLen := len(data[0])
	for ind := 1; ind < len(data); ind++ {
		if len(data[ind]) != dataLen {
			return nil, fmt.Errorf("data not all same length in NewAnyData")
		}
	}

	if len(tableSpec.FieldDefs) != dataLen {
		return nil, fmt.Errorf("tablespec not length of data in NewAnyData")
	}

	ramData = &Reader{
		Data:      data,
		tableSpec: tableSpec,
		onRow:     0,
	}

	return ramData, nil
}

// Read func
func (rdr *Reader) Read(nTarget int, validate bool) (data []chutils.Row, valid []chutils.Valid, err error) {
	if rdr.onRow+1 > len(rdr.Data) {
		return nil, nil, io.EOF
	}

	numFields := len(rdr.Data[0])

	startRow := rdr.onRow
	if nTarget == 0 {
		nTarget = len(rdr.Data)
	}

	limit := startRow + nTarget - 1
	if limit >= len(rdr.Data) {
		limit = len(rdr.Data) - 1
	}

	for row := startRow; row <= limit; row++ {
		rdr.onRow++

		outrow := rdr.Data[row]
		if validate {
			vrow := make(chutils.Valid, numFields)
			outrow = make(chutils.Row, numFields)
			for j := 0; j < numFields; j++ {
				val, stat := rdr.TableSpec().FieldDefs[j].Validator(rdr.Data[row][j])
				outrow[j] = val
				vrow[j] = stat
			}
			valid = append(valid, vrow)
		}

		data = append(data, outrow)
	}

	return data, valid, nil
}

func (rdr *Reader) Reset() error {
	rdr.onRow = 0
	return nil
}

func (rdr *Reader) CountLines() (numLines int, err error) {
	return len(rdr.Data), nil
}

// Seek to a specific row. lineNo starts at 1.
func (rdr *Reader) Seek(lineNo int) error {
	if lineNo > len(rdr.Data) {
		return fmt.Errorf("seek past end of data")
	}

	if lineNo < 1 {
		return fmt.Errorf("illegal value of seek: %d", lineNo)
	}

	rdr.onRow = lineNo - 1

	return nil
}

func (rdr *Reader) Close() error {
	return nil
}

func (rdr *Reader) TableSpec() *chutils.TableDef {
	return rdr.tableSpec
}
