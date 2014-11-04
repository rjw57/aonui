package main

import (
	"flag"
	"fmt"
	aonui "github.com/rjw57/aonui"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const maximumSimultaneousDownloads = 5
const maximumTries = 4

// Global semaphore used to limit the number of simultaneous downloads
var fetchSem = make(chan int, maximumSimultaneousDownloads)

func main() {
	// Command-line flags
	var (
		baseDir string
	)

	// Parse command line
	flag.StringVar(&baseDir, "basedir", ".", "directory to download data to")
	flag.Parse()

	// Fetch all of the runs
	runs, err := aonui.GFSHalfDegreeDataset.FetchRuns()
	if err != nil {
		log.Fatal(err)
	}

	// Sort by *descending* date
	sort.Sort(sort.Reverse(ByDate(runs)))

	// Check that we have found enough runs
	if len(runs) < 2 {
		log.Print("Not enough runs found.")
		return
	}

	// Choose the penultimate run
	run := runs[1]
	log.Print("Fetching data for run at ", run.When)

	// Get datasets for this run
	datasets, err := run.FetchDatasets()
	if err != nil {
		log.Fatal(err)
	}

	// Open the output file
	filename := filepath.Join(baseDir, run.Identifier+".grib2")
	log.Print("Fetching run to ", filename)
	output, err := os.Create(filename)
	if err != nil {
		log.Print("Error creating output: ", err)
		return
	}

	// Ensure the file is closed on function exit
	defer output.Close()

	// Concatenate temporary files as they are finished
	fetchStart := time.Now()
	for fn := range fetchDatasetsData(baseDir, datasets) {
		if f, err := os.Open(fn); err != nil {
			log.Print("Error copying temporary file: ", err)
		} else {
			io.Copy(output, f)
		}
		os.Remove(fn)
	}

	fetchDuration := time.Since(fetchStart)
	fi, err := output.Stat()
	if err != nil {
		log.Print("Error: ", err)
		return
	}
	log.Print(fmt.Sprintf("Overall download speed: %v/sec",
		ByteCount(float64(fi.Size())/fetchDuration.Seconds())))
}

func fetchDatasetsData(baseDir string, datasets []*aonui.Dataset) chan string {
	// Which records are we interested in?
	paramsOfInterest := []string{"HGT", "UGRD", "VGRD"}

	var wg sync.WaitGroup
	tmpFilesChan := make(chan string)

	trySleepDuration, err := time.ParseDuration("10s")
	if err != nil {
		log.Fatal(err)
	}

	for _, ds := range datasets {
		wg.Add(1)

		go func(dataset *aonui.Dataset) {
			defer wg.Done()

			fetchSem <- 1
			defer func() { <-fetchSem }()

			// Create a temporary file for output
			tmpFile, err := ioutil.TempFile(baseDir, "dataset-")
			if err != nil {
				log.Print("Error creating temporary file: ", err)
			}
			defer tmpFile.Close()

			// Perform download. Attempt download repeatedly
			for tries := 0; tries < maximumTries; tries++ {
				log.Print("Fetching ", dataset.Identifier,
					" (try ", tries+1, " of ", maximumTries, ")")
				err := fetchDataset(tmpFile, dataset, paramsOfInterest)
				if err == nil {
					break
				} else {
					log.Print("Error fetching dataset: ", err)
				}

				// Sleep until the next try
				time.Sleep(trySleepDuration)
			}

			tmpFilesChan <- tmpFile.Name()
		}(ds)
	}

	// Launch a goroutine to wait for all datasets to be downloaded and
	// then close the channel.
	go func() {
		wg.Wait()
		close(tmpFilesChan)
	}()

	return tmpFilesChan
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

	log.Print(fmt.Sprintf("Fetching %d records from %v (%v)",
		len(fetchItems), dataset.Identifier, ByteCount(totalToFetch)))
	if _, err := dataset.FetchAndWriteRecords(output, fetchItems); err != nil {
		return err
	}

	return nil
}
