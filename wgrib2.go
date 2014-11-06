// Functions for dealing with wgrib2

package aonui

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

// A GridShape represents the shape of one record within a GRIB2 file.
type GridShape struct {
	Columns, Rows int
}

// Command used for launching wgrib2. On each invocation, this command is
// looked up in the system path.
var Wgrib2Command = "wgrib2"

// Wgrib2Extract uses Wgrib2 to extract a GRIB2 into a direct binary formatted
// file. No headers or other information are added to the file which consists
// of packed native float types in West-to-East, South-to-North,
// record-by-record ordering. Input and output are specified as filenames.
// Which records to extract and their order is specified by inv.
func Wgrib2Extract(inv Inventory, sourceFn string, destFn string) error {
	// Build wgrib2 command
	cmd := exec.Command(Wgrib2Command, "-i", "-no_header", "-bin", destFn, sourceFn)

	// Get stdin pipe
	wg2Stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}

	// Get error pipe
	wg2Stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	// Start command
	if err := cmd.Start(); err != nil {
		return err
	}

	// Write inventory into wgrib2
	go func() {
		for _, item := range inv {
			for _, ln := range item.Wgrib2Strings() {
				fmt.Fprintln(wg2Stdin, ln)
			}
		}
		wg2Stdin.Close()
	}()

	// Copy standard error from wgrib2
	go io.Copy(os.Stderr, wg2Stderr)

	// Wait for command completion
	if err := cmd.Wait(); err != nil {
		return err
	}

	// Return success
	return nil
}

// Wgrib2Inventory uses wgrib2 to parse the inventory of the GRIB2 file
// specified by its filename.
func Wgrib2Inventory(fn string) (Inventory, error) {
	// Get total length of GRIB2 file
	var fi os.FileInfo
	fi, err := os.Stat(fn)
	if err != nil {
		return nil, err
	}
	totalLength := fi.Size()

	// Build wgrib2 command
	cmd := exec.Command(Wgrib2Command, "-s", fn)

	// Get pipes
	wg2Stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	wg2Stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}

	// Start command
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	// Concurrently parse inventory
	invChan, errChan := make(chan Inventory), make(chan error)
	go func() {
		if inv, err := ParseInventory(wg2Stdout, totalLength); err != nil {
			errChan <- err
		} else {
			invChan <- inv
		}
	}()

	// Copy standard error from wgrib2
	go io.Copy(os.Stderr, wg2Stderr)

	// Wait for inventory or parse error
	var (
		inv    Inventory
		invErr error
	)
	select {
	case inv = <-invChan:
		// We have an inventory
	case invErr = <-errChan:
		// Oh, dear
	}

	// Wait for command completion
	if err := cmd.Wait(); err != nil {
		return nil, err
	}

	return inv, invErr
}

// This is the pattern we expect for shape fields
var shapeRegex = regexp.MustCompile(`^\(([0-9]+) x ([0-9]+)\)$`)

// parseShapes will read wgrib2 -nxny output from r sending each parse shape
// along shapeChan. Any errors are passed along errChan. After parsing,
// shapeChan is closed.
func parseShapes(r io.Reader, shapeChan chan GridShape, errChan chan error) {
	// No matter how we exit, close the channel
	defer close(shapeChan)

	// For each line...
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()

		// Split into fields delimited by ":"
		fields := strings.Split(line, ":")

		// Check we have enough
		if len(fields) < 3 {
			errChan <- errors.New("too few fields read from input")
			return
		}

		// Extract shape field
		shapeField := fields[2]

		// Match against pattern
		submatches := shapeRegex.FindStringSubmatch(shapeField)
		if submatches == nil {
			errChan <- errors.New("shape field has wrong format")
			return
		}

		// Get columns and rows
		columns, err := strconv.Atoi(submatches[1])
		if err != nil {
			errChan <- err
			return
		}
		rows, err := strconv.Atoi(submatches[2])
		if err != nil {
			errChan <- err
		}

		shapeChan <- GridShape{Rows: rows, Columns: columns}
	}
}

// Wgrib2GridShapes uses wgrib2 to parse dump the shapes of records
// in sourceFn corresponding to each inventory item in inv.
func Wgrib2GridShapes(inv Inventory, sourceFn string) ([]GridShape, error) {
	// Build wgrib2 command
	cmd := exec.Command(Wgrib2Command, "-i", "-nxny", sourceFn)

	// Get stdin pipe
	wg2Stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	// Get stdin pipe
	wg2Stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	// Get error pipe
	wg2Stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}

	// Create error and shape channels
	errChan, shapeChan := make(chan error), make(chan GridShape)

	// Start command
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	// Write inventory into wgrib2
	go func() {
		for _, item := range inv {
			for _, ln := range item.Wgrib2Strings() {
				fmt.Fprintln(wg2Stdin, ln)
			}
		}
		wg2Stdin.Close()
	}()

	// Copy standard error from wgrib2
	go io.Copy(os.Stderr, wg2Stderr)

	// Parse shapes from wgrib2
	go parseShapes(wg2Stdout, shapeChan, errChan)

	// Wait for shapes or errors
	shapes := []GridShape{}
	var (
		shapeErr  error
		shapeDone bool
	)
	for shapeErr == nil && !shapeDone {
		select {
		case shape, shapeOk := <-shapeChan:
			if shapeOk {
				shapes = append(shapes, shape)
			} else {
				shapeDone = true
			}
		case err := <-errChan:
			shapeErr = err
		}
	}

	// If we had a shape error, report it
	if shapeErr != nil {
		return nil, shapeErr
	}

	// Wait for command completion
	if err := cmd.Wait(); err != nil {
		return nil, err
	}

	// Return success
	return shapes, nil
}
