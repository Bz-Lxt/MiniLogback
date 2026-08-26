package main

import (
	"testing"
	"time"
)

func TestSummarizeAndPercentages(t *testing.T) {
	values := make([]int64, 1000)
	for i := range values {
		values[i] = int64(i + 1)
	}
	result := summarize(values)
	if result.P50NS != 500 || result.P95NS != 950 || result.P99NS != 990 || result.P999NS != 999 || result.MaxNS != 1000 {
		t.Fatalf("unexpected quantiles: %+v", result)
	}
	if percentDrop(100, 75) != 25 || percentRise(100, 125) != 25 {
		t.Fatal("unexpected percentage calculation")
	}
}

func TestShortPublishAndSoakPaths(t *testing.T) {
	published, err := runPublish("off", 10*time.Millisecond, 30*time.Millisecond, 1)
	if err != nil {
		t.Fatal(err)
	}
	if published.Latency.Samples < 100 || published.Attempts != published.Accepted+published.QueueFull+published.RejectedOther || published.Outstanding != 0 {
		t.Fatalf("invalid publish report: %+v", published)
	}
	soak, err := runSoak("off", 20*time.Millisecond, 50*time.Millisecond, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if soak.Accepted == 0 || soak.Attempts != soak.Accepted+soak.QueueFull+soak.RejectedOther || soak.Outstanding != 0 || soak.ThirtyMinuteEvidence {
		t.Fatalf("invalid short soak report: %+v", soak)
	}
}

func TestRunLabelsSoakWithItsActualSingleProducer(t *testing.T) {
	report, err := run(options{Mode: "soak", Audit: "off", Warmup: 10 * time.Millisecond, Duration: 20 * time.Millisecond, Rate: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if report.Config.Producers != 1 {
		t.Fatalf("soak producers = %d; want 1", report.Config.Producers)
	}
}
