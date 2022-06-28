// Package str implements the Input interface for strings by leveraging the file.Reader type.
// This is especially useful in conjunction with http.Get.
// This package also supports reading from Excel XLSX files.
package str

import (
	"fmt"
	"github.com/invertedv/chutils/file"
	"github.com/xuri/excelize/v2"
	"strings"
)

// ReaderCloser just embeds a *strings.Reader
type ReaderCloser struct {
	*strings.Reader
}

// Close implements the Close method for ReaderCloser.
func (r *ReaderCloser) Close() error {
	return nil
}

// NewReader generates a new file.Reader for the string input.
func NewReader(s string, separator rune, eol rune, quote rune, width int, skip int, maxRead int) *file.Reader {
	srdr := strings.NewReader(s)
	rdr := &ReaderCloser{srdr}
	return file.NewReader("", separator, eol, quote, width, skip, maxRead, rdr, 0)
}

// NewXlReader creates a *file.Reader for an Excel spreadsheet.  The sheet is read into a string and then
// initialized using str.NewReader
// xl      -  XL file (stream) to read
// sheet   - sheet to read. If this is empty, the first sheet is read
// rowS    - start row (index is 0-based)
// rowE    - end row (0 means all rows)
// colS    - start column (index is 0-based)
// colE    - end column (0 means all columns)
// quote   - string quotes
// skip    - rows to skip before reading data
// maxRead - max # rows to read
func NewXlReader(xl *excelize.File, sheet string, rowS, rowE, colS, colE int, quote rune, skip int, maxRead int) *file.Reader {
	if sheet == "" {
		sheet = xl.GetSheetName(0)
	}
	s := ""
	indr := 0
	rx, _ := xl.Rows(sheet)
	defer func() { _ = rx.Close() }()
	for rx.Next() {
		if indr >= rowS && (indr <= rowE || rowE == 0) {
			line := make([]byte, 0)
			cx, _ := rx.Columns()
			for indc := range cx {
				if indc >= colS && (indc <= colE || colE == 0) {
					cell := makeCol(indc, indr)
					_ = xl.SetCellStyle(sheet, cell, cell, 0)
					a, _ := xl.GetCellValue(sheet, cell)
					line = append(line, []byte(a)...)
					line = append(line, byte('\t'))
				}
			}
			line[len(line)-1] = byte('\n')
			s += string(line)
		}
		indr++
	}
	return NewReader(s, '\t', '\n', quote, 0, skip, maxRead)
}

// returns the Excel axis address for a cell (e.g. (0,0) -> "A1"
func makeCol(col int, row int) string {
	cols := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	if col < len(cols) {
		return string(cols[col]) + fmt.Sprintf("%d", row+1)
	}
	tens := col / 26
	ones := col % 26
	return string(cols[tens-1]) + string(cols[ones]) + fmt.Sprintf("%d", row+1)

}
