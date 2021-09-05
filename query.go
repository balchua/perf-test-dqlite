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

func doQuery(ctx context.Context, db *sql.DB, runnerId string) {

	ticker := time.NewTicker(10 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			query(ctx, db, runnerId)
		}
	}
}

func query(ctx context.Context, db *sql.DB, runnerId string) {
	d := time.Now().Add(20 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer timeTrack(time.Now(), fmt.Sprintf("query - %s", runnerId))
	defer cancel()
	row := db.QueryRow(queryStatement)
	var result string
	seed := time.Now().UnixNano()
	random := rand.New(rand.NewSource(seed))

	action := func(attempt uint) error {
		var err error
		if err := row.Scan(&result); err != nil {
			log.Printf("Query [%s] Attenpt (%d) - %s", runnerId, attempt, err.Error())
		}
		if err == nil {
			log.Printf("%s - Retrieved successfully %s.\n", runnerId, result)
		}

		return err
	}

	err := retry.Retry(
		action,
		strategy.Limit(2000),
		strategy.BackoffWithJitter(backoff.Linear(10*time.Millisecond), jitter.Deviation(random, 0.8)),
	)

	if err != nil {
		log.Fatalf("Query[%s] Error: %v", runnerId, err.Error())
	}

	//Check context for error, If ctx.Err() != nil gracefully exit the current execution
	if ctx.Err() != nil {
		log.Fatalf("query[%s]- %v \n", runnerId, ctx.Err())
	}

}
