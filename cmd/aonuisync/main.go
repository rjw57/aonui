package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rjw57/aonui"
)

const maximumSimultaneousDownloads = 5

// Global semaphore used to limit the number of simultaneous downloads
var fetchSem = make(chan int, maximumSimultaneousDownloads)

type TemporaryFileSource struct {
	BaseDir string
	Prefix  string

	files []*os.File
}

func (tfs *TemporaryFileSource) Create() (*os.File, error) {
	f, err := ioutil.TempFile(tfs.BaseDir, tfs.Prefix)
	if err != nil {
		return nil, err
	}

	tfs.files = append(tfs.files, f)
	return f, nil
}

func (tfs *TemporaryFileSource) Remove(f *os.File) error {
	// Find index of f in files
	for fIdx := 0; fIdx < len(tfs.files); fIdx++ {
		if tfs.files[fIdx] != f {
			continue
		}

		// We found f, remove it from our list
		tfs.files = append(tfs.files[:fIdx], tfs.files[fIdx+1:]...)

		// Remove it from disk
		if err := os.Remove(f.Name()); err != nil {
			return err
		}
	}

	// If we get here, f was not in files
	return errors.New("Temporary file was not managed by me")
}

func (tfs *TemporaryFileSource) RemoveAll() error {
	var lastErr error

	for _, f := range tfs.files {
		if err := os.Remove(f.Name()); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// Command-line flags
var (
	baseDir string
	highRes bool
	maxRuns int
)

func main() {
	// Parse command line
	flag.StringVar(&baseDir, "basedir", ".", "directory to download data to")
	flag.BoolVar(&highRes, "highres", false, "download 0.25deg data as opposed to 0.5deg")
	flag.IntVar(&maxRuns, "maxruns", 3, "maximum number of runs to examine before giving up")
	flag.Parse()

	// Which source to use?
	src := aonui.GFSHalfDegreeDataset
	if highRes {
		src = aonui.GFSQuarterDegreeDataset
	}

	// Fetch all of the runs
	runs, err := src.FetchRuns()
	if err != nil {
		log.Fatal(err)
	}

	// Sort by *descending* date
	sort.Sort(sort.Reverse(ByDate(runs)))

	succeeded := false
	for _, run := range runs[:maxRuns] {
		destFn := filepath.Join(baseDir, run.Identifier+".grib2")

		if err := syncRun(run, destFn); err != nil {
			log.Print("error syncing run: ", err)

			// ensure we remove destFn if we created it
			if os.IsExist(err) {
				log.Print("Removing ", destFn)
				os.Remove(destFn)
			}
		} else {
			// success!
			log.Print("run downloaded successfully")
			succeeded = true
			break
		}
	}

	if !succeeded {
		log.Fatal("no runs were downloaded")
	}
}

func syncRun(run *aonui.Run, destFn string) error {
	log.Print("Fetching data for run at ", run.When)

	// Get datasets for this run
	datasets, err := run.FetchDatasets()
	if err != nil {
		return err
	}
	log.Print("Run has ", len(datasets), " dataset(s)")

	if len(datasets) < run.Source.MinDatasets {
		log.Print("Run has too few, expecting at least ", run.Source.MinDatasets)
		return errors.New("Too few datasets in source")
	}

	// File source for temporary files
	tfs := TemporaryFileSource{BaseDir: baseDir, Prefix: "dataset-"}
	defer tfs.RemoveAll()

	// Make sure to remove temporary files on keyboard interrupt
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for s := range c {
			log.Printf("captured %v, deleting temporary files", s)
			tfs.RemoveAll()
			os.Exit(1)
		}
	}()

	// Open the output file
	log.Print("Fetching run to ", destFn)
	output, err := os.Create(destFn)
	if err != nil {
		log.Print("Error creating output: ", err)
		return err
	}

	// Ensure the file is closed on function exit
	defer output.Close()

	// Concatenate temporary files as they are finished
	fetchStart := time.Now()
	for f := range fetchDatasetsData(&tfs, datasets) {
		if input, err := os.Open(f.Name()); err != nil {
			log.Print("Error copying temporary file: ", err)
		} else {
			io.Copy(output, input)
			input.Close()
		}
		tfs.Remove(f)
	}

	fetchDuration := time.Since(fetchStart)
	fi, err := output.Stat()
	if err != nil {
		log.Print("Error: ", err)
		return err
	}
	log.Print(fmt.Sprintf("Overall download speed: %v/sec",
		ByteCount(float64(fi.Size())/fetchDuration.Seconds())))

	return nil
}

func fetchDatasetsData(tfs *TemporaryFileSource, datasets []*aonui.Dataset) chan *os.File {
	// Which records are we interested in?
	paramsOfInterest := []string{"HGT", "UGRD", "VGRD"}

	var wg sync.WaitGroup
	tmpFilesChan := make(chan *os.File)

	trySleepDuration, err := time.ParseDuration("10s")
	if err != nil {
		log.Fatal(err)
	}

	for _, ds := range datasets {
		// If we have a max forecast hour, and this dataset is later, skip
		if ds.Run.Source.MaxForecastHour > 0 && ds.ForecastHour > ds.Run.Source.MaxForecastHour {
			continue
		}

		wg.Add(1)

		go func(dataset *aonui.Dataset) {
			defer wg.Done()

			fetchSem <- 1
			defer func() { <-fetchSem }()

			// Perform download. Attempt download repeatedly
			maximumTries := dataset.Run.Source.FetchStrategy.MaximumRetries
			var tmpFile *os.File
			for tries := 0; tries < maximumTries; tries++ {
				// Create a temporary file for output
				tmpFile, err = tfs.Create()
				if err != nil {
					log.Print("Error creating temporary file: ", err)
				}

				log.Print("Fetching ", dataset.Identifier,
					" (try ", tries+1, " of ", maximumTries, ")")
				err := fetchDataset(tmpFile, dataset, paramsOfInterest)
				if err == nil {
					break
				} else {
					log.Print("Error fetching dataset: ", err)
				}

				// Remove this temporary file
				tmpFile.Close()
				tfs.Remove(tmpFile)
				tmpFile = nil

				// Sleep until the next try
				time.Sleep(trySleepDuration)
			}

			if tmpFile == nil {
				log.Print("error: failed to download ", dataset.Identifier)
			} else {
				tmpFile.Close()
				tmpFilesChan <- tmpFile
			}
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

		// HACK: we also are only interested in wind velocities at a
		// particular pressure. (i.e. ones whose "LayerName" field is of
		// the form "XXX mb".)
		saveItem = saveItem && strings.HasSuffix(item.LayerName, " mb")

		if saveItem {
			fetchItems = append(fetchItems, item)
			totalToFetch += item.Extent
		}
	}

	if len(fetchItems) == 0 {
		log.Print("No items to fetch")
		return nil
	}

	log.Print(fmt.Sprintf("Fetching %d records from %v (%v)",
		len(fetchItems), dataset.Identifier, ByteCount(totalToFetch)))
	if _, err := dataset.FetchAndWriteRecords(output, fetchItems); err != nil {
		return err
	}

	return nil
}
