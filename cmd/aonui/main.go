package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
)

// Commands lists the available commands and help topics.
// The order here is the order in which they are printed by 'aonui help'.
var commands = []*Command{
	cmdSync,
	cmdExtract,
	cmdInfo,
	cmdInv,
	cmdReorder,

	helpTawhiri,
}

func main() {
	flag.Usage = usage
	flag.Parse()
	log.SetFlags(0)

	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	if args[0] == "help" {
		help(args[1:])
		return
	}

	// Set signal handler so that "atexit" functions are called on keyboard
	// interrupt.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for s := range c {
			log.Printf("captured %v, cleaning up", s)
			exit()
		}
	}()

	for _, cmd := range commands {
		if cmd.Name() == args[0] && cmd.Run != nil {
			cmd.Flag.Usage = func() { cmd.Usage() }
			if cmd.CustomFlags {
				args = args[1:]
			} else {
				cmd.Flag.Parse(args[1:])
				args = cmd.Flag.Args()
			}
			cmd.Run(cmd, args)
			exit()
			return
		}
	}

	fmt.Fprintf(os.Stderr, "aonui: unknown subcommand %q\nRun 'aonui help' for usage.\n", args[0])
	setExitStatus(2)
	exit()
}

var exitStatus = 0
var exitMu sync.Mutex

func setExitStatus(n int) {
	exitMu.Lock()
	if exitStatus < n {
		exitStatus = n
	}
	exitMu.Unlock()
}

var atexitFuncs []func()

func atexit(f func()) {
	atexitFuncs = append(atexitFuncs, f)
}

func exit() {
	for _, f := range atexitFuncs {
		f()
	}
	os.Exit(exitStatus)
}
