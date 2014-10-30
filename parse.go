// Parsing functions

package aonui

import "code.google.com/p/go.net/html"
import "log"
import "net/url"
import "regexp"
import "strconv"
import "strings"
import "time"

const (
	runsRoot       = "http://ftp.ncep.noaa.gov/data/nccf/com/gfs/prod/"
	runPattern     = `^gfs\.(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})(?P<hour>\d{2})$`
	datasetPattern = `^gfs\.t(?P<runHour>\d{2})z.(?P<typeId>pgrb2b?f)(?P<fcstHour>\d+)$`
)

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
func (ctx *parseRunsContext) matchRunNode(node *html.Node, out chan *Run) {
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
		run := &Run{Identifier: identifier, URL: url, When: when}

		// Send run to output channel
		out <- run
	}
}

func fetchAndParseRuns(runChannel chan *Run) {
	// Ensure channel is closed when this function exits
	defer close(runChannel)

	// Form base URL (should never fail as this is a constant)
	baseURL, err := url.Parse(runsRoot)
	if err != nil {
		log.Fatal(err)
	}

	// Compile regexp for matching run name
	runRegexp, err := regexp.Compile(runPattern)
	if err != nil {
		log.Fatal(err)
		return
	}

	// Fetch runs
	doc, err := getAndParse(runsRoot)
	if err != nil {
		return
	}

	// Walk entire parse tree...
	ctx := &parseRunsContext{BaseURL: baseURL, RunRegexp: runRegexp}
	walkNodeTree(doc, func(node *html.Node) {
		ctx.matchRunNode(node, runChannel)
	})
}

type parseDatasetsContext struct {
	Run           *Run
	DatasetRegexp *regexp.Regexp
}

func (run *Run) fetchAndParseDatasets(c chan *Dataset) {
	// Ensure the channel is closed when this function returns
	defer close(c)

	// Compile regexp for matching dataset name
	datasetRegexp, err := regexp.Compile(datasetPattern)
	if err != nil {
		log.Fatal(err)
		return
	}

	doc, err := getAndParse(run.URL.String())
	if err != nil {
		return
	}

	// Walk parse tree...
	ctx := &parseDatasetsContext{Run: run, DatasetRegexp: datasetRegexp}
	walkNodeTree(doc, func(node *html.Node) {
		ctx.matchDatasetNode(node, c)
	})
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
