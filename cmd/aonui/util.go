package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/rjw57/aonui"
)

// A ByteCount is a number of bytes. It is a wrapper around int64 allow
// human-friendly formatting.
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

// ByDate is used to sort runs by date
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

// A TemporaryFileSource is used to create temporary files, remember such
// creation and then to tidy up afterwards.
type TemporaryFileSource struct {
	BaseDir string
	Prefix  string

	files []*os.File
}

// Create will create a new temporary file and return an os.File for it.
func (tfs *TemporaryFileSource) Create() (*os.File, error) {
	f, err := ioutil.TempFile(tfs.BaseDir, tfs.Prefix)
	if err != nil {
		return nil, err
	}

	tfs.files = append(tfs.files, f)
	return f, nil
}

// Remove will remove a file previously created via Create(). It is an error to
// pass an *os.File which was not created in this way.
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
	return errors.New("temporary file was not managed by me")
}

// RemoveAll will remove all files which have been created by this
// TemporaryFileSource. It is intended that this function be called at exit.
func (tfs *TemporaryFileSource) RemoveAll() error {
	var lastErr error

	for _, f := range tfs.files {
		if err := os.Remove(f.Name()); err != nil {
			lastErr = err
		}
	}

	return lastErr
}
