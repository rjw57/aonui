package main

// Dump the inventory from a GRIB2 file using wgrib2

import (
	"fmt"
	"os"
	"sort"

	"github.com/rjw57/aonui"
)

var cmdInv = &Command{
	UsageLine: "inv [-nosort] [-nofilter] gribfile",
	Short:     "filter and sort GRIB2 inventories into Tawhiri order",
	Long: `
Inv dumps and optionally filters and sorts a GRIB2's inventory into the order
Tawhiri expects. (See "aonui help tawhiri" for details on this ordering.)

The first time this command is run on a file it can take a long time to
generate output as wgrib2 will need to scan through the entire GRIB2 message.

Inv does not directly deal with latitudes or longitudes but will parse the
inventory from the specified GRIB2 file and output an inventory on standard
output which corresponds to the Tawhiri ordering for parameters, pressures and
forecast hours.

Inv will sort inventory items as described above but this behaviour may be
disabled via the -nosort flag.

Inv will remove inventory items not used by Tawhiri. This behaviour can be
disabled via the -nofilter flag. In this case non-Tawhiri inventory
items will be sorted after Tawhiri ones.

With -nosort and -nofilter both enabled, inv should generate an inventory
identical to that produced by "wgrib2 -s".

See also: aonui help tawhiri
`,
}

// Command-line flags
var (
	noSort, noFilter bool
)

func init() {
	cmdInv.Run = runInv // break init loop
	cmdInv.Flag.BoolVar(&noSort, "nosort", false, "Do not sort inventory into \"Tawhiri order\"")
	cmdInv.Flag.BoolVar(&noFilter, "nofilter", false, "Do not remove non-tawhiri items")
}

func runInv(cmd *Command, args []string) {
	// Get file from  line
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "error: exactly one GRIB2 must be specified\n")
		setExitStatus(1)
		return
	}

	// Load and parse inventory
	gribFn := args[0]
	inv, err := aonui.Wgrib2Inventory(gribFn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to parse grib2: %v\n", err)
		setExitStatus(1)
		return
	}

	// Parse items
	tws := aonui.ToTawhiris(inv)

	// Filter invalid records if asked
	if !noFilter {
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
	if !noSort {
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
