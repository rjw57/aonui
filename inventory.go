// Dataset inventories

package aonui

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// An item from a wgrib "short" inventory. We extend the inventory item with a
// computed extent giving the size of the record in bytes.
// See: ftp://ftp.cpc.ncep.noaa.gov/wd51we/wgrib/readme
type InventoryItem struct {
	RecordNumber      int
	Offset            int64
	Extent            int64
	When              time.Time
	Parameters        []string
	LayerName         string
	TypeName          string
	FieldAverageCount int
}

type Inventory []*InventoryItem

// Format an inventory item as a slice of wgrib2-format index records. Specify
// which record within the file this item is via the 0-based idx argument.
func (item *InventoryItem) Wgrib2Strings() []string {
	lines := []string{}
	for pIdx, param := range item.Parameters {
		subParam := ""
		if len(item.Parameters) > 1 {
			subParam = fmt.Sprintf(".%d", pIdx+1)
		}

		fac := ""
		if item.FieldAverageCount != 0 {
			fac = fmt.Sprint(item.FieldAverageCount)
		}

		when := item.When.Format("2006010215")

		line := fmt.Sprintf("%v%v:%d:d=%v:%v:%v:%v:%v",
			item.RecordNumber, subParam, item.Offset, when, param,
			item.LayerName, item.TypeName, fac,
		)
		lines = append(lines, line)
	}
	return lines
}

// Parse a wgrib2-style "short" inventory. Read the inventory from stream. The
// total length of the GRIB2 message should be passed as totalLength.
func ParseInventory(stream io.Reader, totalLength int64) (Inventory, error) {
	var (
		inventory Inventory
		lastItem  *InventoryItem
	)

	// Process each line of the index. We postpone appending the next item
	// from the inventory until we can calculate the extent.
	scanner := bufio.NewScanner(stream)
	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, err
		}

		line := scanner.Text()
		fields := strings.Split(line, ":")
		if len(fields) < 7 {
			return nil, errors.New("Inventory record has too few fields")
		}

		// The record index has one of two formats: "\d+" or
		// "\d+\.\d+". The latter format is used for n-d data (such as
		// wind vectors). We collate these parameters together treating
		// the former case as "\d+\.1".
		subRecord := 1 // default
		recordIds := strings.Split(fields[0], ".")
		if len(recordIds) < 0 || len(recordIds) > 2 {
			return nil, fmt.Errorf("Invalid record number: %v", fields[0])
		}

		// Record number
		record, err := strconv.Atoi(recordIds[0])
		if err != nil {
			return nil, err
		}

		// If present, sub record number
		if len(recordIds) > 1 {
			subRecord, err = strconv.Atoi(recordIds[1])
			if err != nil {
				return nil, err
			}
		}

		// Offset
		offset, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return nil, err
		}

		// Date
		date, err := parseDateField(fields[2])
		if err != nil {
			return nil, err
		}

		// Field average count
		fieldAvCount := 0
		if fields[6] != "" {
			fieldAvCount, err = strconv.Atoi(fields[6])
			if err != nil {
				return nil, err
			}
		}

		if subRecord == 1 {
			// If this is sub-record 1, create a new item as per usual
			item := &InventoryItem{
				RecordNumber:      record,
				Offset:            offset,
				When:              date,
				Parameters:        []string{fields[3]},
				LayerName:         fields[4],
				TypeName:          fields[5],
				FieldAverageCount: fieldAvCount,
			}

			if lastItem != nil {
				lastItem.Extent = item.Offset - lastItem.Offset
				inventory = append(inventory, lastItem)
			}

			lastItem = item
		} else {
			// If this is a later record, update the last item
			if lastItem == nil {
				return nil, errors.New("Unexpected sub-record number >1")
			}

			lastItem.Parameters = append(lastItem.Parameters, fields[3])

			// TODO: Check that the last item matches in all other fields
		}
	}

	// Append the final item
	if lastItem != nil {
		lastItem.Extent = totalLength - lastItem.Offset
		inventory = append(inventory, lastItem)
	}

	return inventory, nil
}

// Parse a string of the form d=YYYYMMDDHH and return a time.Time struct.
func parseDateField(s string) (time.Time, error) {
	re, err := regexp.Compile(`^d=(\d{4})(\d{2})(\d{2})(\d{2})$`)
	if err != nil {
		log.Fatal(err)
	}

	submatches := re.FindStringSubmatch(s)
	if submatches == nil {
		return time.Now(), errors.New("Invalid date field format")
	}

	year, _ := strconv.Atoi(submatches[1])
	month, _ := strconv.Atoi(submatches[2])
	day, _ := strconv.Atoi(submatches[3])
	hour, _ := strconv.Atoi(submatches[4])

	return time.Date(year, time.Month(month), day, hour, 0, 0, 0, time.UTC), nil
}
