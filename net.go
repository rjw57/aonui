// Network-utilities

package aonui

import (
	"errors"
	"log"
	"net/http"
	"time"

	"code.google.com/p/go.net/html"
)

// FetchStrategy represents a strategy for fetching data from servers which may
// be unreliable.
type FetchStrategy struct {
	MaximumRetries int           // Maximum retry count when fetching URLs
	RetrySleep     time.Duration // Time to sleep between tries
	FetchTimeout   time.Duration // Timeout when fetching individual datasets
}

// Fetch data via HTTP with retries and sleep times. Returns http.Response and
// error as per http.Get().
func getURLWithStrategy(url string, strategy FetchStrategy) (*http.Response, error) {
	sleepDuration := strategy.RetrySleep
	nTries := strategy.MaximumRetries
	if nTries < 1 {
		nTries = 1
	}

	// Keep trying
	for try := 0; try < nTries; try++ {
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			// Everything was fine
			return resp, nil
		} else if err == nil {
			// Some non-OK status was returned
			log.Print("HTTP GET returned status ", resp.StatusCode, ", retrying.")
		} else {
			// Some network error happened
			log.Print("HTTP GET returned error: ", err, ". Retrying.")
		}

		time.Sleep(sleepDuration)
	}

	// If we get here, give up.
	return nil, errors.New("maximum number of retries exceeded")
}

// Fetch data from a URL interpreting the result as HTML and return the root of
// the HTML parse tree. Returns an error if the fetch failed.
func getAndParse(url string, strategy FetchStrategy) (*html.Node, error) {
	// Attempt to fetch URL
	log.Print("Fetching ", url)
	resp, err := getURLWithStrategy(url, strategy)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Parse index as HTML
	doc, err := html.Parse(resp.Body)
	if err != nil {
		log.Print("error parsing ", url, ": ", err)
		return nil, err
	}

	return doc, nil
}
