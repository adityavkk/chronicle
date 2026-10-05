// Command checker checks experimental Rust histories offline with Porcupine.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/anishathalye/porcupine"
)

func main() {
	history := flag.String("rust-history", "", "JSONL history to check")
	timeout := flag.Duration("rust-history-timeout", 30*time.Second, "Porcupine search timeout")
	flag.Parse()
	if *history == "" || flag.NArg() != 0 {
		flag.Usage()
		os.Exit(2)
	}
	f, err := os.Open(*history)
	if err == nil {
		defer f.Close()
		var result porcupine.CheckResult
		result, err = checkRustHistory(f, *timeout)
		if err == nil {
			fmt.Println(result)
			if result != porcupine.Ok {
				err = fmt.Errorf("rust history verdict: %s", result)
			}
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
}
