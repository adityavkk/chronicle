package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"gecgithub01.walmart.com/auk000v/chronicle/loadtest/rediscpu"
)

func main() {
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		fail(err)
	}
	summary, err := rediscpu.ParseMonitoringJSON(data)
	if err != nil {
		fail(err)
	}
	out, _ := json.MarshalIndent(summary, "", "  ")
	fmt.Println(string(out))
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "rediscpu:", err)
	os.Exit(1)
}
