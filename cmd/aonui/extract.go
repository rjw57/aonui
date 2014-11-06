package main

import (
	"log"
	"os"

	"github.com/rjw57/aonui"
)

var cmdExtract = &Command{
	Run:       runExtract,
	UsageLine: "extract <ingrib> <outbin>",
	Short:     "extract binary data from a GRIB2 message into Tawhiri order",
	Long: `
Extract will parse a GRIB2 message in the file ingrib and write a raw binary
dump of native-endian floating point values to outbin in Tawhiri order.

See also: aonui help tawhiri
`,
}

func runExtract(cmd *Command, args []string) {
	if len(args) != 2 {
		log.Print("usage: aonui extract <ingrib> <outbin>")
		setExitStatus(1)
		return
	}

	// Get arguments
	sourceFn := args[0]
	destFn := args[1]

	// Do not overwrite existing files
	if _, err := os.Stat(destFn); err == nil {
		log.Fatal("not overwriting existing file ", destFn)
	}

	// Do work
	if err := extract(sourceFn, destFn); err != nil {
		log.Fatal(err)
	}
}

func extract(sourceFn, destFn string) error {
	// Compute tawhiri-ordered inventory
	log.Print("Scanning inventory of ", sourceFn)
	inv, err := aonui.TawhiriOrderedInventory(sourceFn)
	if err != nil {
		return err
	}

	// Expand GRIB
	log.Print("Expanding to ", destFn)
	if err := aonui.Wgrib2Extract(inv, sourceFn, destFn); err != nil {
		return err
	}

	return nil
}
