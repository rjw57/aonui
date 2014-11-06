package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
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

// Command-line flags
var (
	syncBaseDir string
	syncHighRes bool
	syncMaxRuns int
)

var cmdSync = &Command{
	UsageLine: "sync [-basedir directory] [-highres] [-maxruns number]",
	Short:     "fetch wind data from the GFS",
	Long: `
Sync will fetch wind data from the Global Forecast System (GFS) servers in
GRIB2 data. It will only fetch the subset fo the data needed. It knows how to
fetch both the current 0.5 degree resolution data and the forthcoming 0.25
degree data.

Data is saved to the file gfs.YYYMMDDHH.grib2 where YYYY, MM, DD and HH are the
year, month, day and hour of the run with an appropriate number of leading
zeros.

The -basedir option specifies the directory data should be downloaded to. If
omitted, the current working directory is used.

If the -highres option is present, 0.25 degree data will be downloaded. If
omitted, the 0.5 degree data is downloaded.

The -maxruns options controls how far into the past sync will look for data
before stopping. The default value of 3 means examine the 3 newest runs on the
server starting with the newest. If any run is a) incomplete on the server or
b) already downloaded proceed to the next until the list of runs is exhausted.

The utility attempts to be robust in the face of flaky network connections or a
flaky server by re-trying failed downloads.
`,
}

func init() {
	cmdSync.Run = runSync // break init cycle
	cmdSync.Flag.StringVar(&syncBaseDir, "basedir", ".",
		"directory to download data to")
	cmdSync.Flag.BoolVar(&syncHighRes, "highres", false,
		"download 0.25deg data as opposed to 0.5deg")
	cmdSync.Flag.IntVar(&syncMaxRuns, "maxruns", 3,
		"maximum number of runs to examine before giving up")
}

func runSync(cmd *Command, args []string) {
	baseDir, highRes, maxRuns := syncBaseDir, syncHighRes, syncMaxRuns

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

		if _, err := os.Stat(destFn); err == nil {
			log.Print("not overwriting ", destFn)
			continue
		}

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
	tfs := TemporaryFileSource{BaseDir: syncBaseDir, Prefix: "dataset-"}
	defer tfs.RemoveAll()

	// Make sure to remove temporary files on keyboard interrupt
	atexit(func() { tfs.RemoveAll() })

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
