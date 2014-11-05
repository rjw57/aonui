// Re-order a GRIB2 file into Tawhiri order
package main

import (
	"flag"
	"io"
	"log"
	"os"
	"sort"

	"github.com/rjw57/aonui"
)

func main() {
	flag.Parse()

	// Get file from command line
	if len(flag.Args()) != 2 {
		log.Fatal("Usage: aonuireorder <ingrib> <outgrib>")
	}

	outFn := flag.Args()[1]

	// Load and parse inventory
	gribFn := flag.Args()[0]
	inv, err := aonui.Wgrib2Inventory(gribFn)
	if err != nil {
		log.Fatal("error parsing grib2: ", err)
	}

	// Parse items
	tws := aonui.ToTawhiris(inv)

	// Filter invalid records
	filteredTws := []*aonui.TawhiriItem{}
	for _, tw := range tws {
		if tw.IsValid {
			filteredTws = append(filteredTws, tw)
		}
	}
	tws = filteredTws

	// Sort. Note that sorting in this manner is effectively a Swartzian
	// transform.
	sort.Sort(aonui.ByTawhiri(tws))

	// De-parse
	inv = aonui.FromTawhiris(tws)

	// Open input
	in, err := os.Open(gribFn)
	if err != nil {
		log.Fatal(err)
	}
	defer in.Close()

	// Open output
	out, err := os.Create(outFn)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()

	// Perform copy
	for _, invItem := range inv {
		// Seek in input
		in.Seek(invItem.Offset, 0)

		// Copy to output
		_, err := io.CopyN(out, in, invItem.Extent)
		if err != nil {
			log.Fatal(err)
		}
	}
}
