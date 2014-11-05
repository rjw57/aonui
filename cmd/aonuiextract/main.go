// Extract data from GRIB2 file into Tawhiri-ordered binary data file.
package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/rjw57/aonui"
)

func main() {
	var tmpDir string

	flag.StringVar(&tmpDir, "tmpdir", "", "directory to store temporary files in")
	flag.Parse()

	if len(flag.Args()) != 2 {
		log.Fatal("Usage: aonuiextract <ingrib> <outbin>")
	}

	// Get arguments
	sourceFn := flag.Args()[0]
	destFn := flag.Args()[1]

	if tmpDir == "" {
		tmpDir = filepath.Dir(destFn)
	}

	// Create a temporary file
	tmpFile, err := ioutil.TempFile(tmpDir, filepath.Base(destFn)+".reordered.grib2.")
	if err != nil {
		log.Fatal(err)
	}
	tmpFile.Close()
	tmpFn := tmpFile.Name()
	defer func() { log.Print("Removing ", tmpFn); os.Remove(tmpFn) }()

	// Make sure to remove temporary files on keyboard interrupt
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for s := range c {
			log.Printf("captured %v, deleting temporary files", s)
			os.Remove(tmpFn)
			os.Exit(1)
		}
	}()

	log.Print("Re-ordering input GRIB to ", tmpFn)
	if err := aonui.ReorderGrib2(sourceFn, tmpFn); err != nil {
		log.Fatal(err)
	}

	log.Print("Expanding to ", destFn)
	if err := aonui.Wgrib2Extract(tmpFn, destFn); err != nil {
		log.Fatal(err)
	}
}
