package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"
)

type options struct {
	Mode, Audit string
	Duration    time.Duration
	Warmup      time.Duration
	Rate        int
}

func main() {
	var opts options
	flag.StringVar(&opts.Mode, "mode", "publish", "publish or soak")
	flag.StringVar(&opts.Audit, "audit", "off", "off, full, or both (publish only)")
	flag.DurationVar(&opts.Duration, "duration", 10*time.Second, "measured duration (soak release evidence requires 30m)")
	flag.DurationVar(&opts.Warmup, "warmup", 3*time.Second, "unmeasured warmup duration")
	flag.IntVar(&opts.Rate, "rate", 100000, "fixed events/second for soak mode")
	flag.Parse()
	report, err := run(opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "perfcheck:", err)
		os.Exit(2)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		fmt.Fprintln(os.Stderr, "perfcheck: encode report:", err)
		os.Exit(2)
	}
	if !report.Pass {
		os.Exit(1)
	}
}
