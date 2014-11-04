// Parsing functions

package aonui

import "code.google.com/p/go.net/html"
import "log"
import "net/url"
import "regexp"
import "strconv"
import "strings"
import "time"

type nodeFunc func(node *html.Node)

// Walk a HTML parse tree in a depth first manner calling nodeFn for each node.
func walkNodeTree(root *html.Node, nodeFn nodeFunc) {
	// Process root
	nodeFn(root)

	// Walk children concurrently
	for c := root.FirstChild; c != nil; c = c.NextSibling {
		walkNodeTree(c, nodeFn)
	}
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

// Fetch available runs in a dataset. Note that partial runs (i.e. those with
// only some of the datasets uploaded) will also be returned and so one should
// be careful to check the number of datasets matches what you expect.
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

type parseDatasetsContext struct {
	Run           *Run
	DatasetRegexp *regexp.Regexp
}

// Fetch individual datasets from a run.
func (run *Run) FetchDatasets() ([]*Dataset, error) {
	// Compile regexp for matching dataset name
	datasetRegexp, err := regexp.Compile(run.Source.DatasetPattern)
	if err != nil {
		return nil, err
	}

	doc, err := getAndParse(run.URL.String(), run.Source.FetchStrategy)
	if err != nil {
		return nil, err
	}

	// A channel which can receive datasets as they're parsed
	datasetChan := make(chan *Dataset)

	// Walk parse tree...
	ctx := &parseDatasetsContext{Run: run, DatasetRegexp: datasetRegexp}
	go func(c chan *Dataset, ctx *parseDatasetsContext) {
		defer close(c)
		walkNodeTree(doc, func(node *html.Node) {
			ctx.matchDatasetNode(node, c)
		})
	}(datasetChan, ctx)

	// Return datasets
	datasets := []*Dataset{}
	for ds := range datasetChan {
		datasets = append(datasets, ds)
	}

	return datasets, nil
}

func (ctx *parseDatasetsContext) matchDatasetNode(node *html.Node, out chan *Dataset) {
	// Is this node an anchor tag?
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

		// Does this match our pattern for datasets?
		submatches := ctx.DatasetRegexp.FindStringSubmatch(identifier)
		if submatches == nil {
			continue
		}

		// Parse as a relative URL. Skip invalid references
		relURL, err := url.Parse(a.Val)
		if err != nil {
			continue
		}
		url := ctx.Run.URL.ResolveReference(relURL)

		var (
			runHour, forecastHour int
			typeIdentifier        string
		)

		for idx, subexpName := range ctx.DatasetRegexp.SubexpNames() {
			// Also parse submatch as an integer (if possible)
			submatchVal := submatches[idx]
			submatchIntVal, _ := strconv.Atoi(submatchVal)

			// Record matches
			switch subexpName {
			case "runHour":
				runHour = submatchIntVal
			case "fcstHour":
				forecastHour = submatchIntVal
			case "typeId":
				typeIdentifier = submatchVal
			}
		}

		if runHour != ctx.Run.When.Hour() {
			log.Print("Dataset run hour, ", runHour, "does not match run's hour, ",
				ctx.Run.When.Hour())
			continue
		}

		out <- &Dataset{
			Identifier: identifier, URL: url,
			Run: ctx.Run, ForecastHour: forecastHour,
			TypeIdentifier: typeIdentifier,
		}
	}

}
