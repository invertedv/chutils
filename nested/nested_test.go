package nested

import (
	"fmt"
	"github.com/invertedv/chutils"
	"github.com/invertedv/chutils/file"
	"github.com/stretchr/testify/assert"
	"io"
	"log"
	"os"
	"strings"
	"testing"
)

type rstr struct{ strings.Reader }

func (r *rstr) Close() error {
	return nil
}

func NewVars(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	var res string
	switch data[0].(string) {
	case "hello, mom":
		res = "test1"
	case "1":
		res = "test2"
	case "3":
		res = "test3"
	case "5":
		res = "test4"
	default:
		res = "fail"
	}
	return res, nil
}

func TestReader_Read(t *testing.T) {
	input := []string{"a,b\n\"hello, mom\",3\n,\"1\",2\n",
		"a,b\n1,2\n2,3\n",
		"a,b\n3,2\n2,3\n",
		"a,b\n5,2\n6,6\n"}
	quote := []rune{'"', '"', 0, '"'}
	result := []string{"test1", "test2", "test3", "test4"}

	for j := 0; j < len(input); j++ {
		rt := &rstr{*strings.NewReader(input[j])}
		rt1 := file.NewReader("abc", ',', '\n', quote[j], 0, 1, 0, rt, 0)
		rt1.Init()
		fd := &chutils.FieldDef{
			Name:        "validation",
			ChSpec:      chutils.ChField{Base: chutils.ChString},
			Description: "",
			Legal:       chutils.NewLegalValues(),
			Missing:     "!",
			Width:       0,
		}
		newFields := make(map[int]*chutils.FieldDef)
		newFields[0] = fd
		newCalcs := make([]NewCalcFn, 0)
		newCalcs = append(newCalcs, NewVars)
		rt2, err := NewReader(rt1, newFields, newCalcs)
		if err != nil {
			t.Errorf("unexected reader create")
		}
		data, _, err := rt2.Read(1, false)
		if err != nil {
			t.Errorf("unexpected read error")
		}
		assert.Equal(t, data[0][2], result[j])
	}
}

// Example 1
func ExampleReader_Read() {
	/*
		Input file:

		x,y
		1.0,2.0
		3.0,4.0
		100.0, 100.0
	*/

	const myFile = "/home/test/data/input.csv"
	inFile, err := os.Open(myFile)
	if err != nil {
		log.Fatalln(err)
	}
	baseReader := file.NewReader("", ',', '\n', '"', 0, 1, 0, inFile, 0)
	defer baseReader.Close()
	// initialize TableSpec
	if e := baseReader.Init(); e != nil {
		log.Fatalln(e)
	}
	if err := baseReader.TableSpec().Impute(baseReader, 0, .95); err != nil {
		log.Fatalln(err)
	}
	fd := &chutils.FieldDef{
		Name:        "product",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 64},
		Description: "The product of the x and y",
		Legal:       chutils.NewLegalValues(),
		Missing:     -1.0,
		Width:       0,
	}
	fd.Legal.LowLimit, fd.Legal.HighLimit = 0.0, 100.0
	// Create map with new field
	newFields := make(map[int]*chutils.FieldDef)
	newFields[0] = fd
	// Create slice of function to calculate this
	newCalcs := make([]NewCalcFn, 0)
	newCalcs = append(newCalcs,
		func(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
			// this will fail is validate == false since they will be strings
			x, okx := data[0].(float64)
			y, oky := data[1].(float64)
			if !okx || !oky {
				return 0.0, chutils.Wrapper(chutils.ErrInput, "bad inputs to calculation of product")
			}
			return x * y, nil
		})
	// This reader will include our new field "product"
	reader, err := NewReader(baseReader, newFields, newCalcs)
	if err != nil {
		log.Fatalln(err)
	}
	data, _, err := reader.Read(0, true)
	if err != nil && err != io.EOF {
		log.Fatalln(err)
	}
	fmt.Println(data)
	// Output: 		[[1 2 2] [3 4 12] [100 100 -1]]
}

// Example : Column Locations Unknown
func ExampleReader_Read_additional() {
	// If we are unsure of where x and y might be in the CSV, we can find out from the TableSpec
	const myFile = "/home/test/data/input.csv"
	inFile, err := os.Open(myFile)
	if err != nil {
		log.Fatalln(err)
	}
	baseReader := file.NewReader("", ',', '\n', '"', 0, 1, 0, inFile, 0)
	defer baseReader.Close()
	// initialize TableSpec
	if e := baseReader.Init(); e != nil {
		log.Fatalln(e)
	}
	if e := baseReader.TableSpec().Impute(baseReader, 0, .95); e != nil {
		log.Fatalln(e)
	}
	fd := &chutils.FieldDef{
		Name:        "product",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 64},
		Description: "The product of the first two fields",
		Legal:       chutils.NewLegalValues(),
		Missing:     -1.0,
		Width:       0,
	}
	fd.Legal.LowLimit, fd.Legal.HighLimit = 0.0, 100.0
	// Create map with new field
	newFields := make(map[int]*chutils.FieldDef)
	newFields[0] = fd
	// Create slice of function to calculate this
	newCalcs := make([]NewCalcFn, 0)
	newCalcs = append(newCalcs,
		func(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
			// if we don't know where x and y are in the file, we can get their indices
			indx, _, err := td.Get("x")
			if err != nil {
				log.Fatalln(err)
			}
			indy, _, err := td.Get("y")
			if err != nil {
				log.Fatalln(err)
			}
			x, okx := data[indx].(float64)
			y, oky := data[indy].(float64)
			if !okx || !oky {
				return 0.0, chutils.Wrapper(chutils.ErrInput, "bad inputs to calculation of product")
			}
			return x * y, nil
		})
	// This reader will include our new field "product"
	reader, err := NewReader(baseReader, newFields, newCalcs)
	if err != nil {
		log.Fatalln(err)
	}
	data, _, err := reader.Read(0, true)
	if err != nil && err != io.EOF {
		log.Fatalln(err)
	}
	fmt.Println(data)
}
