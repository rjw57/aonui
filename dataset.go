// An individual GRIB dataset from a run.

package aonui

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// A Dataset is a description of an individual GRIB dataset from a run
type Dataset struct {
	Run            *Run
	Identifier     string
	URL            *url.URL
	TypeIdentifier string
	ForecastHour   int
}

// FetchInventory will fetch and parse the GRIB inventory associated with a Dataset. The inventory URL is constructed from the Dataset URL and is not guaranteed to exist.
func (ds *Dataset) FetchInventory() (Inventory, error) {
	// Fetch headers for the actual dataset. This is required to get the
	// complete length.
	resp, err := http.Head(ds.URL.String())
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP error when fetching dataset headers: %d",
			resp.StatusCode)
	}

	// Record and verify the content length
	datasetLength := resp.ContentLength
	if datasetLength < 0 {
		return nil, errors.New("Server did not give Content-Length for dataset")
	}

	// Fetch the inventory
	resp, err = http.Get(ds.InventoryURL().String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP error when fetching inventory: %d", resp.StatusCode)
	}

	// Parse inventory
	return ParseInventory(resp.Body, datasetLength)
}

// InventoryURL will return the URL which is *assumed* to point to the
// inventory in wgrib2 "short" format
func (ds *Dataset) InventoryURL() *url.URL {
	inURL := *ds.URL // NB: Copy of ds.URL
	inURL.Path = inURL.Path + ".idx"
	return &inURL
}

// FetchAndWriteRecords fetches a set of records from an individual dataset and
// writes them sequentially to an io.Writer.
func (ds *Dataset) FetchAndWriteRecords(output io.Writer, records []*InventoryItem) (int64, error) {
	// Create a new HTTP client since we'll be adding custom headers
	client := new(http.Client)

	// Create specific request
	req, err := http.NewRequest("GET", ds.URL.String(), nil)
	if err != nil {
		return 0, err
	}

	// Add a Range header to request specifying which bytes we require.
	rangeSpecs := []string{}
	for _, r := range records {
		// Note that the range is *inclusive*.
		rangeSpec := fmt.Sprintf("%d-%d", r.Offset, r.Offset+r.Extent-1)
		rangeSpecs = append(rangeSpecs, rangeSpec)
	}
	req.Header.Add("Range", "bytes="+strings.Join(rangeSpecs, ","))

	// Fire off request
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	// Check we get partial content
	if resp.StatusCode != http.StatusPartialContent {
		return 0, fmt.Errorf("Expected HTTP partial content, got %v", resp.StatusCode)
	}

	// Everything looks good, start copying
	return io.Copy(output, resp.Body)
}
