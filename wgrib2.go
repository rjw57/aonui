// Functions for dealing with wgrib2

package aonui

import (
	"io"
	"os"
	"os/exec"
)

// Command used for launching wgrib2. On each invocation, this command is
// looked up in the system path.
var Wgrib2Command = "wgrib2"

// Use wgrib2 to parse the inventory of the GRIB2 file specified by its
// filename.
func Wgrib2Inventory(fn string) (Inventory, error) {
	// Get total length of GRIB2 file
	totalLength := int64(0)
	if fi, err := os.Stat(fn); err != nil {
		return nil, err
	} else {
		totalLength = fi.Size()
	}

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
	go func() { io.Copy(os.Stderr, wg2Stderr) }()

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
