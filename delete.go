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

func doDelete(ctx context.Context, db *sql.DB, runnerId string) {

	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ticker.C:
			delete(ctx, db, runnerId)
		}
	}
}

func delete(ctx context.Context, db *sql.DB, runnerId string) {
	d := time.Now().Add(30 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer timeTrack(time.Now(), fmt.Sprintf("delete - %s", runnerId))
	defer cancel()
	now := time.Now()
	nanos := now.UnixNano()
	var result sql.Result
	var rowsAffected int64
	seed := time.Now().UnixNano()
	random := rand.New(rand.NewSource(seed))

	action := func(attempt uint) error {
		var err error
		if result, err = db.Exec(deleteStatement, nanos); err != nil {
			log.Printf("Delete [%s] Attempt (%d) - %s", runnerId, attempt, err.Error())
		}
		if result != nil {
			rowsAffected, err = result.RowsAffected()
			if err != nil {
				log.Printf("Unable to perform delete - %s.", runnerId)
			}
			log.Printf("%s - Deleted successfully [%d].\n", runnerId, rowsAffected)
		}
		return err
	}

	err := retry.Retry(
		action,
		strategy.Limit(2000),
		strategy.BackoffWithJitter(backoff.Linear(10*time.Millisecond), jitter.Deviation(random, 0.8)),
	)

	if err != nil {
		log.Fatalf("Delete [%s] Error: %v", runnerId, err.Error())
	}

	//Check context for error, If ctx.Err() != nil gracefully exit the current execution
	if ctx.Err() != nil {
		log.Fatalf("delete [%s] - %v \n", runnerId, ctx.Err())
	}

}
