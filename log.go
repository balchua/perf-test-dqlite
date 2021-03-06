package main

import (
	"log"
	"time"
)

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("TIMETRACKING: %s took %s", name, elapsed)
}
