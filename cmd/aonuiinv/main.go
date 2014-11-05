// Dump the inventory from a GRIB2 file using wgrib2
package main

import (
	"flag"
	"fmt"
	"log"
	"sort"

	"github.com/rjw57/aonui"
)

func main() {
	var doSort, doFilter bool

	flag.BoolVar(&doSort, "sort", true, "Sort inventory into \"tawhiri\" order")
	flag.BoolVar(&doFilter, "filter", true, "Remove non-tawhiri items")
	flag.Parse()

	// Get file from  line
	if len(flag.Args()) != 1 {
		log.Fatal("Exactly one GRIB2 must be specified")
	}

	// Load and parse inventory
	gribFn := flag.Args()[0]
	inv, err := aonui.Wgrib2Inventory(gribFn)
	if err != nil {
		log.Fatal("error parsing grib2: ", err)
	}

	// Parse items
	tws := aonui.ToTawhiris(inv)

	// Filter invalid records if asked
	if doFilter {
		filteredTws := []*aonui.TawhiriItem{}
		for _, tw := range tws {
			if tw.IsValid {
				filteredTws = append(filteredTws, tw)
			}
		}
		tws = filteredTws
	}

	// Sort if asked. Note that sorting in this manner is effectively a
	// Swartzian transform.
	if doSort {
		sort.Sort(aonui.ByTawhiri(tws))
	}

	// De-parse
	inv = aonui.FromTawhiris(tws)

	// Print inventory
	for _, item := range inv {
		for _, ln := range item.Wgrib2Strings() {
			fmt.Println(ln)
		}
	}
}
