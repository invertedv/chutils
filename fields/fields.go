package fields

import (
	_ "embed"
	"fmt"
	"strings"

	"github.com/invertedv/chutils"
	"github.com/invertedv/keyval"
)

var (
	//go:embed keys.txt
	fieldKeys string
)

// conv converts x to the type specified by field
func conv(x *keyval.Value, fieldType chutils.ChField) (xOut any) {
	const (
		b32, b64 = 32, 64
	)

	xOut = nil
	if x == nil {
		return xOut
	}

	switch fieldType.Base {
	case chutils.ChString:
		return x.AsString
	case chutils.ChInt:
		if x.AsInt == nil {
			return
		}

		switch fieldType.Length {
		case b32:
			xOut = int32(*x.AsInt)
		case b64:
			xOut = int64(*x.AsInt)
		}
	case chutils.ChFloat:
		if x.AsFloat == nil {
			return
		}

		switch fieldType.Length {
		case b32:
			xOut = float32(*x.AsFloat)
		case b64:
			xOut = *x.AsFloat
		}
	case chutils.ChDate:
		if x.AsDate == nil {
			return nil
		}

		xOut = x.AsDate
	case chutils.ChFixedString:
		xOut = x.AsString
		if len(xOut.(string)) != fieldType.Length {
			return nil
		}
	}

	return xOut
}

// kv2Fld creates a field from kv
func kv2Fld(kv keyval.KeyVal) (fd *chutils.FieldDef, order int, err error) {
	order = *kv.Get("order").AsInt
	fd = &chutils.FieldDef{
		Name:        strings.ReplaceAll(kv.Get("name").AsString, " ", ""),
		ChSpec:      chutils.ChField{},
		Description: "",
		Legal:       &chutils.LegalValues{},
		Missing:     nil,
		Default:     nil,
		Width:       0,
		Drop:        false,
	}

	switch kv.Get("type").AsString {
	case "string":
		fd.ChSpec = chutils.ChField{Base: chutils.ChString}
		if lvl := kv.Get("levels"); lvl != nil {
			fd.Legal.Levels = lvl.AsSliceS
		}
		fd.Default = conv(kv.Get("default"), fd.ChSpec)
	case "fixedString":
		fd.ChSpec = chutils.ChField{Base: chutils.ChString, Length: *kv.Get("length").AsInt}
		if lvl := kv.Get("levels"); lvl != nil {
			fd.Legal.Levels = lvl.AsSliceS
		}
		fd.Default = conv(kv.Get("default"), fd.ChSpec)
	case "float":
		fd.ChSpec = chutils.ChField{Base: chutils.ChFloat, Length: *kv.Get("length").AsInt}
		fd.Legal.LowLimit, fd.Legal.HighLimit = conv(kv.Get("low"), fd.ChSpec), conv(kv.Get("high"), fd.ChSpec)
		fd.Missing, fd.Default = conv(kv.Get("miss"), fd.ChSpec), conv(kv.Get("default"), fd.ChSpec)
	case "int":
		fd.ChSpec = chutils.ChField{Base: chutils.ChInt, Length: *kv.Get("length").AsInt}
		fd.Legal.LowLimit, fd.Legal.HighLimit = conv(kv.Get("low"), fd.ChSpec), conv(kv.Get("high"), fd.ChSpec)
		fd.Missing, fd.Default = conv(kv.Get("miss"), fd.ChSpec), conv(kv.Get("default"), fd.ChSpec)
	case "date":
		fd.ChSpec = chutils.ChField{Base: chutils.ChDate, Format: kv.Get("format").AsString}
		fd.Legal.LowLimit, fd.Legal.HighLimit = conv(kv.Get("low"), fd.ChSpec), conv(kv.Get("high"), fd.ChSpec)
		fd.Missing, fd.Default = conv(kv.Get("miss"), fd.ChSpec), conv(kv.Get("default"), fd.ChSpec)
		fd.ChSpec.Format = "20060102" // default, may be replaced by user
	}

	if drop := kv.Get("drop"); drop != nil {
		fd.Drop = drop.AsString == "yes"
	}

	if desc := kv.Get("desc"); desc != nil {
		fd.Description = desc.AsString
	}

	if format := kv.Get("format"); format != nil {
		fd.ChSpec.Format = format.AsString
	}

	return fd, order, nil
}

func BuildFieldDefs(fields string) (map[int]*chutils.FieldDef, error) {
	fds := make(map[int]*chutils.FieldDef)

	fldsX := strings.Split(fields, "\n")
	var flds []string
	for ind := 0; ind < len(fldsX); ind++ {
		if len(strings.ReplaceAll(fldsX[ind], " ", "")) > 0 {
			flds = append(flds, fldsX[ind])
		}
	}

	var key, val, kv []string
	process, eof := false, false
	for ind := 0; ind < len(flds); ind++ {
		var (
			fldDefs keyval.KeyVal
			e       error
		)

		eof = ind+1 == len(flds)
		kv = strings.SplitN(flds[ind], ":", 2)
		kv[0] = strings.ReplaceAll(kv[0], " ", "")
		kv[1] = strings.TrimRight(strings.TrimLeft(kv[1], " "), " ")

		process = (kv[0] == "name")

		if !process || eof {
			key = append(key, kv[0])
			val = append(val, kv[1])
		}

		if (process || eof) && key != nil {
			if fldDefs, e = keyval.ProcessKVs(key, val); e != nil {
				return nil, e
			}
			if e := keyval.CheckLegals(fldDefs, fieldKeys); e != nil {
				return nil, e
			}

			fd, ord, err := kv2Fld(fldDefs)
			if err != nil {
				return nil, err
			}

			fds[ord] = fd
		}

		if process && !eof {
			key = []string{kv[0]}
			val = []string{kv[1]}
		}
	}

	for ind := 0; ind < len(fds); ind++ {
		if _, ok := fds[ind]; !ok {
			return nil, fmt.Errorf("incorrect field ordering")
		}
	}

	return fds, nil
}
