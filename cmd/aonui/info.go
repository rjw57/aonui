package main

import (
	"fmt"
	"log"
	"sort"

	"github.com/rjw57/aonui"
)

var cmdInfo = &Command{
	Run:       runInfo,
	UsageLine: "info gribfile",
	Short:     "print information on GRIB2 file",
	Long: `
Info prints information on the shape of data in a GRIB2 file to standard
output. Gribfile specifies which GRIB2 file is parsed. Output has the following
form:

	NX=720
	NY=361
	NPARAM=3
	NPRESSURE=47
	NFCSTHOUR=65
	PRESSURES=1000,975,950,925,900,875,850,... # etc
	FCSTHOURS=0,3,6,9,12,15,18,21,24,27,30,... # etc

NX, NY, NPARAM, NPRESSURE and NFCSTHOUR give the sizes of each dimension of the
data. PRESSURES and FCSTHOURS are comma-separated integers giving the
particular pressures and forecast hours which correspondt to each point along
the respective axes.
`,
}

func runInfo(cmd *Command, args []string) {
	if len(args) != 1 {
		log.Print("error: no GRIB file specified")
		setExitStatus(1)
		return
	}

	gribFn := args[0]

	// Get inventory from grib
	inv, err := aonui.TawhiriOrderedInventory(gribFn)
	if err != nil {
		log.Print(err)
		setExitStatus(1)
		return
	}

	// Check for empty file
	if len(inv) == 0 {
		log.Print("error: empty GRIB")
		setExitStatus(1)
		return
	}

	// Form a map of forecast hours and pressures.
	fcstHourMap := make(map[int]bool)
	pressureMap := make(map[int]bool)

	// For each tawhiri item in the inventory...
	for _, twItem := range aonui.ToTawhiris(inv) {
		// skip invalid items
		if !twItem.IsValid {
			continue
		}

		// set pressure and forecast hour flag
		fcstHourMap[twItem.ForecastHour] = true
		pressureMap[twItem.Pressure] = true
	}

	// Form a list of forecast hours and pressures
	var fcstHours, pressures []int
	for k := range fcstHourMap {
		fcstHours = append(fcstHours, k)
	}
	for k := range pressureMap {
		pressures = append(pressures, k)
	}

	// Sort forecast hours and pressures
	sort.Ints(fcstHours)
	sort.Sort(sort.Reverse(sort.IntSlice(pressures)))

	// Get shapes from grib
	// HACK: only look at first item
	shapes, err := aonui.Wgrib2GridShapes(inv[:1], gribFn)
	if err != nil {
		log.Print(err)
		setExitStatus(1)
		return
	}
	if len(shapes) < 1 {
		log.Print("error: no grids in GRIB?!")
		setExitStatus(1)
		return
	}

	fmt.Printf("NX=%d\n", shapes[0].Columns)
	fmt.Printf("NY=%d\n", shapes[0].Rows)
	fmt.Printf("NPARAM=3\n")
	fmt.Printf("NPRESSURE=%d\n", len(pressures))
	fmt.Printf("NFCSTHOUR=%d\n", len(fcstHours))

	fmt.Print("PRESSURES=")
	for idx, p := range pressures {
		if idx != 0 {
			fmt.Print(",")
		}
		fmt.Print(p)
	}
	fmt.Print("\n")

	fmt.Print("FCSTHOURS=")
	for idx, fh := range fcstHours {
		if idx != 0 {
			fmt.Print(",")
		}
		fmt.Print(fh)
	}
	fmt.Print("\n")
}
