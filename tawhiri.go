package aonui

import (
	"strconv"
	"strings"
)

// Support for ordering and filtering inventories to contain records that
// tawhiri expects.

type TawhiriItem struct {
	Item         *InventoryItem
	ForecastHour int
	Pressure     int
	ParamIdx     int // HGT = 0, UGRD = 1, VGRD = 2, Other = 3

	// Only true if ForecastHour and Pressure were parsed without error
	IsValid bool
}

func ToTawhiri(item *InventoryItem) *TawhiriItem {
	const (
		fcstSuffix     = " hour fcst"
		pressureSuffix = " mb"
	)

	transItem := &TawhiriItem{Item: item, IsValid: true}

	// Is this a geopotential height record?
	if len(item.Parameters) > 0 {
		switch item.Parameters[0] {
		case "HGT":
			transItem.ParamIdx = 0
		case "UGRD":
			transItem.ParamIdx = 1
		case "VGRD":
			transItem.ParamIdx = 2
		}
	} else {
		transItem.ParamIdx = 3
	}

	// Parse forecast hour
	if item.TypeName == "anl" {
		// anl == "0" forecast hour
		transItem.ForecastHour = 0
	} else if strings.HasSuffix(item.TypeName, fcstSuffix) {
		// parse initial part
		valStr := strings.TrimSuffix(item.TypeName, fcstSuffix)
		var err error
		if transItem.ForecastHour, err = strconv.Atoi(valStr); err != nil {
			// error parsing
			transItem.IsValid = false
		}
	} else {
		transItem.IsValid = false
	}

	// Parse pressure
	if strings.HasSuffix(item.LayerName, pressureSuffix) {
		valStr := strings.TrimSuffix(item.LayerName, pressureSuffix)
		var err error
		if transItem.Pressure, err = strconv.Atoi(valStr); err != nil {
			// error parsing
			transItem.IsValid = false
		}
	} else {
		transItem.IsValid = false
	}

	return transItem
}

func FromTawhiri(item *TawhiriItem) *InventoryItem { return item.Item }

func ToTawhiris(items Inventory) []*TawhiriItem {
	out := []*TawhiriItem{}
	for _, i := range items {
		out = append(out, ToTawhiri(i))
	}
	return out
}

func FromTawhiris(items []*TawhiriItem) Inventory {
	out := Inventory{}
	for _, i := range items {
		out = append(out, FromTawhiri(i))
	}
	return out
}

type ByTawhiri []*TawhiriItem

func (a ByTawhiri) Len() int      { return len(a) }
func (a ByTawhiri) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByTawhiri) Less(i, j int) bool {
	i1, i2 := a[i], a[j]

	if i1.IsValid {
		// Invalid items sort after valid ones always
		if !i2.IsValid {
			return true
		}
	} else {
		// Two invalid items or i1 invalid, i2 valid return false In
		// the case of two invalid items, treat all items as equal and
		// in the case of i1 invalid, i2 valid, i2 should come before
		// i1.
		return false
	}

	// Sort initially by forecast hour
	if i1.ForecastHour < i2.ForecastHour {
		return true
	}
	if i1.ForecastHour > i2.ForecastHour {
		return false
	}

	// Then by *descending* pressure
	if i1.Pressure > i2.Pressure {
		return true
	}
	if i1.Pressure < i2.Pressure {
		return false
	}

	// Then by parameters
	if i1.ParamIdx < i2.ParamIdx {
		return true
	}
	if i1.ParamIdx > i2.ParamIdx {
		return false
	}

	// Otherwise, treat as equal
	return false
}
