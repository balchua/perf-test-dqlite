package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/Rican7/retry"
	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/jitter"
	"github.com/Rican7/retry/strategy"
)

func insert(ctx context.Context, db *sql.DB, runnerId string) {
	d := time.Now().Add(60 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer timeTrack(time.Now(), fmt.Sprintf("insert - %s", runnerId))
	defer cancel()
	now := time.Now()
	nanos := now.UnixNano()
	seed := time.Now().UnixNano()
	random := rand.New(rand.NewSource(seed))
	action := func(attempt uint) error {
		var err error
		if _, err = db.Exec(update, nanos, "anyvalue"); err != nil {
			log.Printf("Insert [%s](attempt %d) - %s", runnerId, attempt, err.Error())
		}
		return err
	}

	err := retry.Retry(
		action,
		strategy.Limit(2000),
		strategy.BackoffWithJitter(backoff.Linear(10*time.Millisecond), jitter.Deviation(random, 0.8)),
	)

	if err != nil {
		log.Fatalf("Insert [%s] Error: %v", runnerId, err.Error())
	}

	//Check context for error, If ctx.Err() != nil gracefully exit the current execution
	if ctx.Err() != nil {
		log.Fatalf("insert [%s] - %v \n", runnerId, ctx.Err())
	}

	log.Printf("%s - Inserted and committed successfully.\n", runnerId)

}

func doInsert(ctx context.Context, db *sql.DB, runnerId string) {

	ticker := time.NewTicker(10 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			insert(ctx, db, runnerId)
		}
	}
}
