// Package aonui provides support for downloading subsets of the Global
// Forecast System runs in GRIB format.
package aonui

import "net/url"
import "time"

// Fetching available runs from the GFS asynchronously and
// return a channel along which run information is passed which is closed after
// the run index has been fetched.
func FetchRuns() []*Run {
	c := make(chan *Run)

	// Launch a goroutine to fetch the run index, walk the document tree
	// and return runs parsed out from it.
	go fetchAndParseRuns(c)

	runs := []*Run{}
	for r := range c {
		runs = append(runs, r)
	}

	return runs
}

// A description of an individual run of the GFS.
type Run struct {
	Identifier string
	URL        *url.URL
	When       time.Time
}

// Fetching available datasets associated with this run.
func (run *Run) FetchDatasets() []*Dataset {
	c := make(chan *Dataset)

	// Launch a goroutine to fetch the run index, walk the document tree
	// and return datasets parsed out from it.
	go run.fetchAndParseDatasets(c)

	datasets := []*Dataset{}
	for ds := range c {
		datasets = append(datasets, ds)
	}

	return datasets
}
