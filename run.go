// Individual runs of the GFS.

package aonui

import (
	"log"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"code.google.com/p/go.net/html"
)

// A Run is a description of an individual run of the GFS.
type Run struct {
	Source     *DataSource
	Identifier string
	URL        *url.URL
	When       time.Time
}

// FetchDatasets fetches a list of individual datasets from a run.
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

type parseDatasetsContext struct {
	Run           *Run
	DatasetRegexp *regexp.Regexp
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
