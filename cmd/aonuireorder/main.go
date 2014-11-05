// Re-order a GRIB2 file into Tawhiri order
package main

import (
	"flag"
	"log"

	"github.com/rjw57/aonui"
)

func main() {
	flag.Parse()

	// Get file from command line
	if len(flag.Args()) != 2 {
		log.Fatal("Usage: aonuireorder <ingrib> <outgrib>")
	}

	gribFn := flag.Args()[0]
	outFn := flag.Args()[1]

	if err := aonui.ReorderGrib2(gribFn, outFn); err != nil {
		log.Fatal(err)
	}
}
