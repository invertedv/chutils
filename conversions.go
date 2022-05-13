package chutils

import (
	"fmt"
	"reflect"
	"strconv"
	"time"
)

// Convert converts the type of inValue to the type specified by fd.
// returns the converted value and a boolean == true if successful
func convert(inValue interface{}, fd ChField) (interface{}, bool) {
	switch fd.Base {
	case ChFloat:
		switch fd.Length {
		case 64:
			funcs := []func(x any) (float64, bool){f64f64, f32f64, strf64, int32f64, int64f64, datef64, intf64}
			if ind, ok := kindIndex(inValue); ok {
				return funcs[ind](inValue)
			}
		case 32:
			funcs := []func(x any) (float32, bool){f64f32, f32f32, strf32, int32f32, int64f32, datef32, intf32}
			if ind, ok := kindIndex(inValue); ok {
				return funcs[ind](inValue)
			}
		}
	case ChInt:
		switch fd.Length {
		case 64:
			funcs := []func(x any) (int64, bool){f64int64, f32int64, strint64, int32int64, int64int64, dateint64, intint64}
			if ind, ok := kindIndex(inValue); ok {
				return funcs[ind](inValue)
			}
		case 32:
			funcs := []func(x any) (int32, bool){f64int32, f32int32, strint32, int32int32, int64int32, dateint32, intint32}
			if ind, ok := kindIndex(inValue); ok {
				return funcs[ind](inValue)
			}
		}
	case ChString, ChFixedString:
		funcs := []func(x any) (string, bool){f64str, f32str, strstr, int32str, int64str, datestr, intstr}
		if ind, ok := kindIndex(inValue); ok {
			return funcs[ind](inValue)
		}
	case ChDate:

		funcs := []func(x any, dfmt string) (time.Time, bool){f64date, f32date, strdate, int32date, int64date, datedate, intdate}
		if ind, ok := kindIndex(inValue); ok {
			return funcs[ind](inValue, fd.DateFormat)
		}
	}
	return nil, false
}

// kindIndex returns the index into the funcs slice for the appropriate inValue type
func kindIndex(inValue any) (int, bool) {
	vo := reflect.ValueOf(inValue)
	vr := vo.Kind()
	switch vr {
	case reflect.Float64:
		return 0, true
	case reflect.Float32:
		return 1, true
	case reflect.String:
		return 2, true
	case reflect.Int32:
		return 3, true
	case reflect.Int64:
		return 4, true
	case reflect.Int:
		return 6, true
	default:
		_, ok := inValue.(time.Time)
		if ok {
			return 5, true
		}
	}
	return -1, false
}

// Conversion functions
// to float64
func f64f64(x any) (float64, bool) { return x.(float64), true }
func f32f64(x any) (float64, bool) { return float64(x.(float32)), true }
func strf64(x any) (float64, bool) {
	f, err := strconv.ParseFloat(x.(string), 64)
	return f, err == nil
}
func int32f64(x any) (float64, bool) { return float64(x.(int32)), true }
func int64f64(x any) (float64, bool) { return float64(x.(int64)), true }
func datef64(x any) (float64, bool)  { _ = x; return 0.0, false }
func intf64(x any) (float64, bool)   { return float64(x.(int)), true }

// to float32
func f64f32(x any) (float32, bool) { return float32(x.(float64)), true }
func f32f32(x any) (float32, bool) { return x.(float32), true }
func strf32(x any) (float32, bool) {
	f, err := strconv.ParseFloat(x.(string), 32)
	return float32(f), err == nil
}
func int32f32(x any) (float32, bool) { return float32(x.(int32)), true }
func int64f32(x any) (float32, bool) { return float32(x.(int64)), true }
func datef32(x any) (float32, bool)  { _ = x; return 0.0, false }
func intf32(x any) (float32, bool)   { return float32(x.(int)), true }

// to int32
func f64int32(x any) (int32, bool) { return int32(x.(float64)), true }
func f32int32(x any) (int32, bool) { return int32(x.(float32)), true }
func strint32(x any) (int32, bool) {
	f, err := strconv.ParseInt(x.(string), 10, 32)
	return int32(f), err == nil
}
func int32int32(x any) (int32, bool) { return x.(int32), true }
func int64int32(x any) (int32, bool) { return int32(x.(int64)), true }
func dateint32(x any) (int32, bool)  { _ = x; return 0, false }
func intint32(x any) (int32, bool)   { return int32(x.(int)), true }

// to int64
func f64int64(x any) (int64, bool) { return int64(x.(float64)), true }
func f32int64(x any) (int64, bool) { return int64(x.(float32)), true }
func strint64(x any) (int64, bool) {
	f, err := strconv.ParseInt(x.(string), 10, 64)
	return f, err == nil
}
func int32int64(x any) (int64, bool) { return int64(x.(int32)), true }
func int64int64(x any) (int64, bool) { return x.(int64), true }
func dateint64(x any) (int64, bool)  { _ = x; return 0, false }
func intint64(x any) (int64, bool)   { return int64(x.(int)), true }

// to string
func f64str(x any) (string, bool)   { return fmt.Sprintf("%v", x.(float64)), true }
func f32str(x any) (string, bool)   { return fmt.Sprintf("%v", x.(float32)), true }
func strstr(x any) (string, bool)   { return x.(string), true }
func int32str(x any) (string, bool) { _ = x; return fmt.Sprintf("%v", x.(int32)), true }
func int64str(x any) (string, bool) { return fmt.Sprintf("%v", x.(int64)), true }
func datestr(x any) (string, bool) {
	return x.(time.Time).Format("2006-01-02"), true
}
func intstr(x any) (string, bool) { _ = x; return fmt.Sprintf("%v", x.(int)), true }

// to time.Time
func f64date(x any, dfmt string) (time.Time, bool) { _, _ = x, dfmt; return DateMissing, false }
func f32date(x any, dfmt string) (time.Time, bool) { _, _ = x, dfmt; return DateMissing, false }
func strdate(x any, dfmt string) (time.Time, bool) {
	f, err := time.Parse(dfmt, x.(string))
	return f, err == nil
}
func int32date(x any, dfmt string) (time.Time, bool) {
	f, err := time.Parse(dfmt, fmt.Sprintf("%v", x.(int32)))
	return f, err == nil
}
func int64date(x any, dfmt string) (time.Time, bool) {
	f, err := time.Parse(dfmt, fmt.Sprintf("%v", x.(int64)))
	return f, err == nil
}
func datedate(x any, dfmt string) (time.Time, bool) { _ = dfmt; return x.(time.Time), true }
func intdate(x any, dfmt string) (time.Time, bool) {
	f, err := time.Parse(dfmt, fmt.Sprintf("%v", x.(int)))
	return f, err == nil
}

// Iterator iterates through an interface that contains a slice,
type iterator struct {
	Item     interface{} // Item holds the current item in the slice
	NewItems interface{} // NewItems is an optional interface holding a slice of the same type
	data     interface{} // data is the interface iterated through
	ind      int         // ind is the index of the current element
}

func newIterator(data interface{}) *iterator {
	return &iterator{data: data}
}

// Append appends an item to a new slice that has the same underlying type as the data
func (i *iterator) Append(v interface{}) {
	switch i.data.(type) {
	case []float32:
		if i.NewItems == nil {
			i.NewItems = make([]float32, 0)
		}
		i.NewItems = append(i.NewItems.([]float32), v.(float32))
		return
	case []float64:
		if i.NewItems == nil {
			i.NewItems = make([]float64, 0)
		}
		i.NewItems = append(i.NewItems.([]float64), v.(float64))
		return
	case []int32:
		if i.NewItems == nil {
			i.NewItems = make([]int32, 0)
		}
		i.NewItems = append(i.NewItems.([]int32), v.(int32))
		return
	case []int64:
		if i.NewItems == nil {
			i.NewItems = make([]int64, 0)
		}
		i.NewItems = append(i.NewItems.([]int64), v.(int64))
		return
	case []string:
		if i.NewItems == nil {
			i.NewItems = make([]string, 0)
		}
		i.NewItems = append(i.NewItems.([]string), v.(string))
		return
	case []time.Time:
		if i.NewItems == nil {
			i.NewItems = make([]time.Time, 0)
		}
		i.NewItems = append(i.NewItems.([]time.Time), v.(time.Time))
		return
	}
}

// Next populates Iterator.item to the next element of the array as an interface{}. Returns false if there are no more elements.
func (i *iterator) Next() bool {
	i.Item = nil
	switch i.data.(type) {
	case []float32:
		v := i.data.([]float32)
		if i.ind == len(v) {
			return false
		}
		i.Item = v[i.ind]
	case []float64:
		v := i.data.([]float64)
		if i.ind == len(v) {
			return false
		}
		i.Item = v[i.ind]
	case []int32:
		v := i.data.([]int32)
		if i.ind == len(v) {
			return false
		}
		i.Item = v[i.ind]
	case []int64:
		v := i.data.([]int64)
		if i.ind == len(v) {
			return false
		}
		i.Item = v[i.ind]
	case []time.Time:
		v := i.data.([]time.Time)
		if i.ind == len(v) {
			return false
		}
		i.Item = v[i.ind]
	case []string:
		v := i.data.([]string)
		if i.ind == len(v) {
			return false
		}
		i.Item = v[i.ind]
	}
	i.ind++
	return true
}

// Compare compares the values of two fields that are stored as interface{}
// returns true if left >= right
func compare(left interface{}, right interface{}, ch ChField) bool {
	if left == nil || right == nil {
		return true
	}
	switch ch.Base {
	case ChFloat:
		switch ch.Length {
		case 32:
			return left.(float32) >= right.(float32)
		case 64:
			return left.(float64) >= right.(float64)
		}
	case ChInt:
		switch ch.Length {
		case 32:
			return left.(int32) >= right.(int32)
		case 64:
			return left.(int64) >= right.(int64)
		}
	case ChDate:
		return left.(time.Time).Sub(right.(time.Time)) >= 0
	case ChString, ChFixedString:
		return left.(string) >= right.(string)
	}
	return false
}
