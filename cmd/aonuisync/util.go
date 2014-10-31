package main

import (
	"aonui"
	"fmt"
)

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
