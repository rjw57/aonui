package main

// Re-order a GRIB2 file into Tawhiri order

import (
	"fmt"
	"os"

	"github.com/rjw57/aonui"
)

var cmdReorder = &Command{
	Run:       runReorder,
	UsageLine: "reorder ingribfile outgribfile",
	Short:     "re-order a GRIB2 file into Tawhiri order",
	Long: `
Reorder will take an existing GRIB2 file on disk and write out a new GRIB2 file
with the records re-ordered into the order Tawhiri expects. (See "aonui help
tawhiri" for details on this ordering.)

Input is read from ingribfile and written to outgribfile. Records not used by
Tawhiri will not be written to the output.

See also: aonui help tawhiri
`,
}

func runReorder(cmd *Command, args []string) {
	// Get file from command line
	if len(args) != 2 {
		fmt.Fprintf(os.Stderr, "input and output grib file must be specified\n")
		setExitStatus(1)
		return
	}

	gribFn := args[0]
	outFn := args[1]

	if err := aonui.ReorderGrib2(gribFn, outFn); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		setExitStatus(1)
		return
	}
}
