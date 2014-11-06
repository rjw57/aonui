package main

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/rjw57/aonui"
)

var cmdExtract = &Command{
	UsageLine: "extract [-tmpdir directory] <ingrib> <outbin>",
	Short:     "extract binary data from a GRIB2 message into Tawhiri order",
	Long: `
Extract will parse a GRIB2 message in the file ingrib and write a raw binary
dump of native-order floating point values to outbin.

If the -tmpdir option is specified, it gives a directory in which a temporary
GRIB2 file in the correct format is first generated. If omitted, the directory
containing outbin is used.

See also: aonui help tawhiri
`,
}

var (
	extractTmpDir string
)

func init() {
	cmdExtract.Run = runExtract // break init cycle
	cmdExtract.Flag.StringVar(&extractTmpDir, "tmpdir", "",
		"directory to store temporary files in")
}

func runExtract(cmd *Command, args []string) {
	if len(args) != 2 {
		log.Print("usage: aonui extract <ingrib> <outbin>")
		setExitStatus(1)
		return
	}

	// Get arguments
	sourceFn := args[0]
	destFn := args[1]
	tmpDir := extractTmpDir
	if tmpDir == "" {
		tmpDir = filepath.Dir(destFn)
	}

	// Do not overwrite existing files
	if _, err := os.Stat(destFn); err == nil {
		log.Fatal("not overwriting existing file ", destFn)
	}

	// Do work
	if err := extract(sourceFn, destFn, tmpDir); err != nil {
		log.Fatal(err)
	}
}

func extract(sourceFn, destFn, tmpDir string) error {
	// Create a temporary file
	tmpFile, err := ioutil.TempFile(tmpDir, filepath.Base(destFn)+".reordered.grib2.")
	if err != nil {
		return err
	}
	tmpFile.Close()
	tmpFn := tmpFile.Name()
	defer func() { log.Print("Removing ", tmpFn); os.Remove(tmpFn) }()

	// Make sure to remove temporary files on exit
	atexit(func() {
		log.Printf("deleting temporary files")
		os.Remove(tmpFn)
	})

	log.Print("Re-ordering input GRIB to ", tmpFn)
	if err := aonui.ReorderGrib2(sourceFn, tmpFn); err != nil {
		return err
	}

	log.Print("Expanding to ", destFn)
	if err := aonui.Wgrib2Extract(tmpFn, destFn); err != nil {
		return err
	}

	return nil
}
