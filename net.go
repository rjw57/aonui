// Network-utilities

package aonui

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"code.google.com/p/go.net/html"
)

// FetchStrategy represents a strategy for fetching data from servers which may
// be unreliable.
type FetchStrategy struct {
	// Maximum retry count when fetching URLs
	MaximumRetries int

	// Time to sleep between tries in seconds
	TrySleepSeconds int
}

// TrySleepDuration will return a time.Duration which represents the number of
// seconds in the TrySleepSeconds field.
func (s *FetchStrategy) TrySleepDuration() time.Duration {
	d, err := time.ParseDuration(fmt.Sprint(s.TrySleepSeconds, "s"))
	if err != nil {
		log.Fatal("Unexpected error parsing duration: ", err)
	}
	return d
}

// Fetch data via HTTP with retries and sleep times. Returns http.Response and
// error as per http.Get().
func getURLWithStrategy(url string, strategy FetchStrategy) (*http.Response, error) {
	sleepDuration := strategy.TrySleepDuration()
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
