package main

import "aonui"
import "fmt"
import "io"
import "log"
import "os"
import "path/filepath"
import "sort"
import "time"

// Sorting runs by date
type ByDate []*aonui.Run

func (d ByDate) Len() int {
	return len(d)
}

func (d ByDate) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func (d ByDate) Less(i, j int) bool {
	return d[i].When.Before(d[j].When)
}

func main() {
	// Fetch all of the runs
	runs := aonui.FetchRuns()

	// Sort by *descending* date
	sort.Sort(sort.Reverse(ByDate(runs)))

	if len(runs) < 2 {
		log.Print("Not enough runs found.")
		return
	}

	// Choose the penultimate run
	run := runs[1]
	log.Print("Fetching data for run at", run.When)

	// Which records are we interested in?
	paramsOfInterest := []string{"HGT", "UGRD", "VGRD"}

	baseDir := "/localdata/rjw57/cusf/aonui"

	// Open the output file
	filename := filepath.Join(baseDir, run.Identifier)
	log.Print("Fetching run to ", filename)
	output, err := os.Create(filename)
	if err != nil {
		log.Print("Error creating output: ", err)
		return
	}

	// Ensure the file is closed on function exit
	defer output.Close()

	for _, dataset := range run.FetchDatasets() {
		err := fetchDataset(output, dataset, paramsOfInterest)
		if err != nil {
			log.Print("Error fetching dataset: ", err)
		}
	}
}

type ByteCount int64

func (bytes ByteCount) String() string {
	switch {
	case bytes < 2<<10:
		return fmt.Sprintf("%dB", bytes)
	case bytes < 2<<20:
		return fmt.Sprintf("%dKiB", bytes>>10)
	case bytes < 2<<30:
		return fmt.Sprintf("%dMiB", bytes>>20)
	default:
		return fmt.Sprintf("%dGiB", bytes>>30)
	}
}

func fetchDataset(output io.Writer, dataset *aonui.Dataset, paramsOfInterest []string) error {
	// Fetch inventory for this dataset
	inventory, err := dataset.FetchInventory()
	if err != nil {
		return err
	}

	// Calculate which items to save
	var (
		totalToFetch int64 = 0
		fetchItems   []*aonui.InventoryItem
	)
	for _, item := range inventory {
		saveItem := false
		for _, poi := range paramsOfInterest {
			for _, p := range item.Parameters {
				saveItem = saveItem || poi == p
			}
		}
		if saveItem {
			fetchItems = append(fetchItems, item)
			totalToFetch += item.Extent
		}
	}

	log.Print("Fetching dataset: ", dataset.URL)
	log.Print(fmt.Sprintf("Fetching %d records (%v)", len(fetchItems), ByteCount(totalToFetch)))
	start := time.Now()
	fetched, err := dataset.FetchAndWriteRecords(output, fetchItems)
	if err != nil {
		return err
	}
	fetchSpeed := int64(float64(fetched) / time.Since(start).Seconds())
	log.Print(fmt.Sprintf("Fetched at %v/sec", ByteCount(fetchSpeed)))

	return nil
}
