package aonui

import (
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"code.google.com/p/go.net/html"
)

// A DataSource contains information on where to get runs and datasets from
type DataSource struct {
	Root            string        // Root URL for dataset
	RunPattern      string        // Pattern to match directories containing individual runs
	DatasetPattern  string        // Pattern to match individual datasets within a run
	FetchStrategy   FetchStrategy // Strategy to use when fetching data
	MaxForecastHour int           // Maximum forecast hour to fetch (or 0 to fetch all)
	MinDatasets     int           // Minimum number of datasets to be "good" (or 0 for no limit)
}

// FetchRuns will fetch available runs in a dataset. Note that partial runs
// (i.e. those with only some of the datasets uploaded) will also be returned
// and so one should be careful to check the number of datasets matches what
// you expect.
func (ds *DataSource) FetchRuns() ([]*Run, error) {
	// Form base URL
	baseURL, err := url.Parse(ds.Root)
	if err != nil {
		return nil, err
	}

	// Compile regexp for matching run name
	runRegexp, err := regexp.Compile(ds.RunPattern)
	if err != nil {
		return nil, err
	}

	// Fetch runs
	doc, err := getAndParse(ds.Root, ds.FetchStrategy)
	if err != nil {
		return nil, err
	}

	// A channel which can receive runs as they're parsed
	runChan := make(chan *Run)

	// Walk entire parse tree...
	ctx := &parseRunsContext{BaseURL: baseURL, RunRegexp: runRegexp}
	go func(c chan *Run, ds *DataSource, ctx *parseRunsContext) {
		defer close(c)
		walkNodeTree(doc, func(node *html.Node) {
			ctx.matchRunNode(node, ds, c)
		})
	}(runChan, ds, ctx)

	// Return runs
	runs := []*Run{}
	for ds := range runChan {
		runs = append(runs, ds)
	}

	return runs, nil
}

// Context used when walking index of GFS runs
type parseRunsContext struct {
	BaseURL   *url.URL
	RunRegexp *regexp.Regexp
}

// Parse an individual node from a HTML parse tree looking for an anchor
// pointing to a GFS run. If the node is a GFS run, send the run along out.
func (ctx *parseRunsContext) matchRunNode(node *html.Node, ds *DataSource, out chan *Run) {
	// Is this node an anchor tag? If not, just return signalling completion.
	if node.Type != html.ElementNode || node.Data != "a" {
		return
	}

	// Look for href attribure
	for _, a := range node.Attr {
		if a.Key != "href" {
			continue
		}

		// Trim any trailing slash
		identifier := strings.TrimRight(a.Val, "/")

		// Does this match our pattern for runs?
		submatches := ctx.RunRegexp.FindStringSubmatch(identifier)
		if submatches == nil {
			continue
		}

		// Parse as a relative URL. Skip invalid references
		relURL, err := url.Parse(a.Val)
		if err != nil {
			continue
		}
		url := ctx.BaseURL.ResolveReference(relURL)

		var year, month, day, hour int
		for idx, subexpName := range ctx.RunRegexp.SubexpNames() {
			// Parse submatch as an integer (if possible)
			submatchVal, err := strconv.Atoi(submatches[idx])
			if err != nil {
				continue
			}

			// If parsing succeeds, update match appropriately
			switch subexpName {
			case "year":
				year = submatchVal
			case "month":
				month = submatchVal
			case "day":
				day = submatchVal
			case "hour":
				hour = submatchVal
			}
		}

		when := time.Date(year, time.Month(month), day, hour, 0, 0, 0, time.UTC)
		run := &Run{Source: ds, Identifier: identifier, URL: url, When: when}

		// Send run to output channel
		out <- run
	}
}
