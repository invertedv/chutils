// Package nested allows for additional calculations to be added to the output of a chutils.Input reader.
// The motivation is that there may be additional fields that need to be calculated from the source data before
// it is handed off to a chutils.Output writer.
//
// The Reader defined here implements chutils.Input.
// Examples of usage:
//  - Adding a field based on which input fields passed validation.
//  - Adding additional fields calculated from the existing inputs.
//  - Adding additional fields based on other variables using a function closure.
//  - Modifying existing fields
package nested

import (
	"github.com/invertedv/chutils"
	"io"
)

// NewCalcFn defines the signature of a function that calculates a new field.
// Note that NewCalcFn can also modify values within data
type NewCalcFn func(ts *chutils.TableDef, data chutils.Row, valid chutils.Valid, validatge bool) (interface{}, error)

// Reader struc that implements chutils.Input.
//  Note that r cannot be embedded because we need to have both r.Read and Reader.Read
type Reader struct {
	r         chutils.Input     // Input before new fields
	tableSpec *chutils.TableDef // tableSpec includes orginal and calculated fields
	newCalcs  []NewCalcFn       // newCalcs is an array of functions to populate calculated fields
}

// NewReader creates a new Reader from
//   - rdr a base reader that satisfies chutils.Input.
//   - newFields an array that defines the additional fields
//   - newCalcs an array of functions that populate the additional fields
func NewReader(rdr chutils.Input, newFields map[int]*chutils.FieldDef, newCalcs []NewCalcFn) (*Reader, error) {
	if len(newFields) != len(newCalcs) {
		return nil, chutils.Wrapper(chutils.ErrFieldCount, "# new fields != # new calc functions")
	}
	fd := make(map[int]*chutils.FieldDef)
	fdExist := rdr.TableSpec().FieldDefs
	nExist := len(fdExist)
	for ind := 0; ind < nExist; ind++ {
		fd[ind] = fdExist[ind]
	}
	for ind := 0; ind < len(newFields); ind++ {
		fd[ind+nExist] = newFields[ind]
	}
	ts := &chutils.TableDef{
		Key:       rdr.TableSpec().Key,
		Engine:    rdr.TableSpec().Engine,
		FieldDefs: fd,
	}
	return &Reader{r: rdr, tableSpec: ts, newCalcs: newCalcs}, nil
}

func (rdr *Reader) TableSpec() *chutils.TableDef {
	return rdr.tableSpec
}

// Read reads nTarget rows from the underlying reader -- Reader.r -- and adds calculated fields.
// Validation is performed if validate == true.  Note: if validate == false, the return from r.Read are strings
func (rdr *Reader) Read(nTarget int, validate bool) (data []chutils.Row, valid []chutils.Valid, err error) {

	data = nil
	valid = nil
	data, valid, errRead := rdr.r.Read(nTarget, validate)
	// no data, return EOF
	if len(data) == 0 {
		return nil, nil, io.EOF
	}
	// non EOF error
	if errRead != nil && errRead != io.EOF {
		return nil, nil, err
	}
	// have data but may also be EOF
	var outValue interface{}

	// number of fields coming in from r
	nExist := len(data[0])
	for row := 0; row < len(data); row++ {
		for ind := 0; ind < len(rdr.newCalcs); ind++ {
			indBig := ind + nExist
			if !validate {
				outValue, err = rdr.newCalcs[ind](rdr.tableSpec, data[0], nil, validate)
				if err != nil {
					return nil, nil, err
				}
				data[row] = append(data[row], outValue)
				break
			}
			outValue, err = rdr.newCalcs[ind](rdr.tableSpec, data[row], valid[row], validate)
			if err != nil {
				return nil, nil, err
			}
			outValueVal, status := rdr.TableSpec().FieldDefs[indBig].Validator(outValue)
			data[row] = append(data[row], outValueVal)
			valid[row] = append(valid[row], status)
		}
	}
	return data, valid, errRead
}

// CountLines returns the number of lines in the underlying reader, Reader.r
func (rdr *Reader) CountLines() (numLines int, err error) {
	return rdr.r.CountLines()
}

// Reset resets the underlying reader, Reader.r
func (rdr *Reader) Reset() error {
	return rdr.r.Reset()
}

// Seek moves the next line to lineNo in the underlying reader, Reader.r
func (rdr *Reader) Seek(lineNo int) error {
	return rdr.r.Seek(lineNo)
}

// Close closes the underlying reader, Reader.r
func (rdr *Reader) Close() error {
	return rdr.r.Close()
}
