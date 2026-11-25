// Package main implements a test runner which orchestrates starting up
// and tearing down servers in the background, and ensuring they are ready before continuing.
package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"time"

	"github.com/please-build/mettle/cli"
)

var log = cli.MustGetLogger()

var opts struct {
	Logging     cli.LoggingOpts `group:"Options controlling logging output"`
	Plz         string          `short:"p" long:"plz" default:"./pleasew" description:"Please binary to run"`
	Interactive bool            `short:"i" long:"interactive" description:"Use interactive output"`
}

func Main() error {
	log.Notice("Building servers...")
	outputFlag := "-p"
	if opts.Interactive {
		outputFlag = "--interactive_output"
	}
	// Build the servers first (so we don't wait for ports to open while we're actually compiling)
	plz := exec.Command(opts.Plz, outputFlag, "-v", "notice", "buildlocal")
	plz.Stdout = os.Stdout
	plz.Stderr = os.Stderr
	if err := plz.Run(); err != nil {
		return err
	}

	// Start the servers in the background and keep them running as we go
	log.Notice("Running servers...")
	f, err := os.Create("plz-out/log/mettle_test.log")
	if err != nil {
		return err
	}
	defer f.Close()
	servers := exec.Command(opts.Plz, outputFlag, "runlocal")
	servers.Stdout = f
	servers.Stderr = f
	if err := servers.Start(); err != nil {
		return err
	}
	defer servers.Process.Signal(os.Interrupt)

	if err := checkPort("7772"); err != nil {
		return err
	} else if err := checkPort("7776"); err != nil {
		return err
	}
	// TODO(peterebden): This is awful, we likely need to build some retrying into plz around this RPC.
	time.Sleep(3 * time.Second)

	log.Notice("Running tests...")
	plz = exec.Command(opts.Plz, "--profile", "localremote", "test", "//tests/...", outputFlag, "-v", "notice", "--log_file", "plz-out/log/tests.log", "-o", "cache.dir:")
	plz.Stdout = os.Stdout
	plz.Stderr = os.Stderr
	return plz.Run()
}

func checkPort(port string) error {
	const tries = 20
	for i := 0; i < tries; i++ {
		conn, err := net.Dial("tcp", "127.0.0.1:"+port)
		if err != nil {
			log.Notice("Waiting for port %s to open...", port)
			time.Sleep(time.Second)
			continue
		}
		conn.Close()
		return nil
	}
	return fmt.Errorf("Failed to find open port %s after %d tries", port, tries)
}

func main() {
	cli.ParseFlagsOrDie("Mettle Test Runner", &opts, &opts.Logging)
	if err := Main(); err != nil {
		log.Fatalf("%s", err)
	}
}
