package chutils

import (
	"io"
	"testing"
	"time"
)

type mockRead struct {
	s        [][]string
	RowsRead int
	name     string
}

func (m *mockRead) Read(nTarget int, validate bool) (data []Row, err error) {
	err = nil
	data = nil
	_ = validate // validate tested separately
	if m.RowsRead < len(m.s) {
		for r := 0; r < nTarget; r++ {
			d := make(Row, 0)
			for ind := 0; ind < len(m.s[m.RowsRead]); ind++ {
				d = append(d, m.s[m.RowsRead][ind])
			}
			data = append(data, d)
			m.RowsRead++
		}
		return
	}
	return nil, io.EOF
}

func (m *mockRead) Reset() error {
	m.RowsRead = 0
	return nil
}

func (m *mockRead) CountLines() (numLines int, err error) {
	m.RowsRead = 0
	return len(m.s), nil
}

func (m *mockRead) Seek(lineNo int) error {
	if lineNo > len(m.s) {
		return io.EOF
	}
	m.RowsRead = lineNo
	return nil
}

func (m *mockRead) Close() error {
	return nil
}

func buildTableDef() TableDef {
	td := TableDef{
		Key:       "a",
		Engine:    MergeTree,
		FieldDefs: nil,
	}

	fd := make(map[int]*FieldDef)
	fnames := []string{"xstr", "xint", "xflt", "xdt"}
	for ind := 0; ind < len(fnames); ind++ {
		fdx := &FieldDef{
			Name:        fnames[ind],
			ChSpec:      ChField{},
			Description: "",
			Legal:       NewLegalValues(),
			Missing:     nil,
			Calculator:  nil,
			Width:       0,
		}
		fd[ind] = fdx
	}
	td.FieldDefs = fd
	return td

}

func TestTableDef_Impute(t *testing.T) {
	data := [][]string{{"a", "1", "2.2", "1/2/2020"},
		{"c", "2", "3.3", "3/1/2002"},
		{"d", "3", "4.4", "12/31/1999"},
		{"f", "4", "5.5", "6/7/2010"}}
	rdr := mockRead{
		s:        data,
		RowsRead: 0,
		name:     "test",
	}
	td := buildTableDef()
	if e := td.Impute(&rdr, 0, .95); e != nil {
		t.Errorf("unexpected Impute error")
	}
	expected := []ChType{ChString, ChInt, ChFloat, ChDate}
	for ind, fd := range td.FieldDefs {
		if fd.ChSpec.Base != expected[ind] {
			t.Errorf("expected type %v got type %v", expected[ind], fd.ChSpec.Base)
		}
	}
}

func TestFieldDef_Validator(t *testing.T) {
	td := buildTableDef()
	types := []ChType{ChInt, ChFloat, ChString, ChDate}
	highs := []interface{}{3, 5.5, ""}
	lows := []interface{}{0, 1.0, ""}
	inputs := [][]interface{}{{"1", "a", "1.4", "111"},
		{"1.1", "a", "-10.0"},
		{"hello", "abc"},
		{"1/2/2020", "2020/06/07", "1/2/2006", "1999/01/01"},
	}
	expected := [][]Status{
		{VPass, VTypeFail, VTypeFail, VValueFail},
		{VPass, VTypeFail, VValueFail},
		{VValueFail, VPass},
		{VTypeFail, VPass, VTypeFail, VValueFail},
	}
	fd := td.FieldDefs[0]
	for r, ty := range types {
		fd.ChSpec.Base = ty
		switch ty {
		case ChInt, ChFloat:
			fd.Legal.HighLimit, fd.Legal.LowLimit = highs[r], lows[r]
		case ChString:
			levels := make(map[string]int)
			levels["abc"] = 1
			levels["def"] = 1
			fd.Legal.Levels = &levels
		case ChDate:
			fd.ChSpec.DateFormat = "2006/01/02"
			dt := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
			fd.Legal.FirstDate = &dt
			dt1 := time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC)
			fd.Legal.LastDate = &dt1

		}
		for c := 0; c < len(inputs[r]); c++ {
			_, status := fd.Validator(inputs[r][c], &td, nil, VPending)
			if status != expected[r][c] {
				t.Errorf("expected %v got %v", expected[r][c], status)
			}
		}
	}
}
