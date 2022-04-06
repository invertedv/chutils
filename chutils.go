package chutils

type Status int

const (
	Pass Status = 0 + iota
	Low
	High
	Invalid
	Calculated
	Pending
)

var status = []string{
	"Pass",
	"Low",
	"High",
	"Invalid",
	"Calculated",
	"Pending",
}

func (s Status) String() string {
	return status[s]
}

type Fields map[string]*Field

type Field struct {
	Name        string
	Value       interface{}
	ChType      string
	Description string
	// replace Levels, HighLimit, LowLimit by interface?
	Levels     []string
	LowLimit   float64
	HighLimit  float64
	Calculator func(fs Fields) interface{}
	Validation Status
}

func (f *Field) Validator(fs Fields) {
	result := Pending
	switch val := f.Value.(type) {
	case string:
		result = Invalid
		for _, r := range f.Levels {
			if val == r {
				result = Pass
				break
			}
		}
	case float64:
		result = Pass
		if val < f.LowLimit {
			result = Low
		} else {
			if val > f.HighLimit {
				result = High
			}
		}
	default:
		result = Pending
	}
	if result != Pass && f.Calculator != nil && f.Validation != Calculated {
		f.Value = f.Calculator(fs)
		f.Validation = Calculated
		f.Validator(fs)
		return
	}
	f.Validation = result
}
