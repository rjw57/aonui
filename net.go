// Network-utilities

package aonui

import "code.google.com/p/go.net/html"
import "fmt"
import "log"
import "net/http"

// Fetch data from a URL interpreting the result as HTML and return the root of
// the HTML parse tree. Also returns an error if the result of the fetch is not
// HTTP 200.
func getAndParse(url string) (*html.Node, error) {
	// Attempt to fetch URL
	log.Print("Fetching: ", url)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Print("error fetching ", url, ": ", err)
		return nil, fmt.Errorf("error fetching %v: HTTP %d", url, resp.StatusCode)
	}

	// Parse index as HTML
	doc, err := html.Parse(resp.Body)
	if err != nil {
		log.Print("error parsing ", url, ": ", err)
		return nil, err
	}

	return doc, nil
}
