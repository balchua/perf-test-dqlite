package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/Rican7/retry"
	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/client"
)

const (
	schema          = "CREATE TABLE IF NOT EXISTS model (key NUMBER, value TEXT, UNIQUE(key))"
	queryStatement  = "SELECT count(key) FROM model"
	update          = "INSERT OR REPLACE INTO model(key, value) VALUES(?, ?)"
	deleteStatement = "DELETE from model where key < ?"
)

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("TIMETRACKING: %s took %s", name, elapsed)
}

func query(ctx context.Context, db *sql.DB, runnerId string) {
	d := time.Now().Add(2 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer timeTrack(time.Now(), fmt.Sprintf("query - %s", runnerId))
	defer cancel()
	row := db.QueryRow(queryStatement)
	var result string

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
		strategy.Limit(20),
		strategy.Backoff(backoff.Linear(200*time.Millisecond)),
	)

	if err != nil {
		log.Fatalf("Query[%s] Error: %v", runnerId, err.Error())
	}

	//Check context for error, If ctx.Err() != nil gracefully exit the current execution
	if ctx.Err() != nil {
		log.Fatalf("query[%s]- %v \n", runnerId, ctx.Err())
	}

}

func insert(ctx context.Context, db *sql.DB, runnerId string) {
	d := time.Now().Add(20 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer timeTrack(time.Now(), fmt.Sprintf("insert - %s", runnerId))
	defer cancel()
	now := time.Now()
	nanos := now.UnixNano()

	action := func(attempt uint) error {
		var err error
		if _, err = db.Exec(update, nanos, "anyvalue"); err != nil {
			log.Printf("Insert [%s](attempt %d) - %s", runnerId, attempt, err.Error())
		}
		return err
	}

	err := retry.Retry(
		action,
		strategy.Limit(20),
		strategy.Backoff(backoff.Linear(200*time.Millisecond)),
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

func delete(ctx context.Context, db *sql.DB, runnerId string) {
	d := time.Now().Add(30 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer timeTrack(time.Now(), fmt.Sprintf("delete - %s", runnerId))
	defer cancel()
	now := time.Now()
	nanos := now.UnixNano()
	var result sql.Result
	var rowsAffected int64

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
		strategy.Limit(20),
		strategy.Backoff(backoff.Linear(200*time.Millisecond)),
	)

	if err != nil {
		log.Fatalf("Delete [%s] Error: %v", runnerId, err.Error())
	}

	//Check context for error, If ctx.Err() != nil gracefully exit the current execution
	if ctx.Err() != nil {
		log.Fatalf("delete [%s] - %v \n", runnerId, ctx.Err())
	}

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

func doDelete(ctx context.Context, db *sql.DB, runnerId string) {

	ticker := time.NewTicker(600 * time.Second)
	for {
		select {
		case <-ticker.C:
			delete(ctx, db, runnerId)
		}
	}
}

func doQuery(ctx context.Context, db *sql.DB, runnerId string) {

	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			query(ctx, db, runnerId)
		}
	}
}

func isLeader(ctx context.Context, app app.App) (bool, error) {
	leaderIp := findLeader(ctx, app)

	if strings.HasPrefix(leaderIp, "127.0.0.1") {
		return true, nil
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		return false, err
	}
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			return false, err
		}
		for _, addr := range addrs {

			switch v := addr.(type) {
			case *net.IPNet:
				if leaderIp == v.IP.String() {
					return true, nil
				}
			case *net.IPAddr:
				if leaderIp == v.IP.String() {
					return true, nil
				}
			}

		}
	}
	return false, nil
}

func findLeader(ctx context.Context, app app.App) string {
	client, err := app.Leader(ctx)

	if err != nil {
		//do nothing
	}

	nodeInfo, err := client.Leader(ctx)

	if err != nil {
		//do nothing
	}
	log.Printf("Leader id [%d] address [%s]\n", nodeInfo.ID, nodeInfo.Address)
	return nodeInfo.Address
}

func main() {
	fmt.Println("Start")
	dir := "/tmp/node1"
	address := "127.0.0.1:6666" // Unique node address
	var err error
	logFunc := func(l client.LogLevel, format string, a ...interface{}) {
		log.Printf(fmt.Sprintf("%s\n", format), a...)
	}

	app, err := app.New(dir, app.WithAddress(address), app.WithCluster(nil), app.WithLogFunc(logFunc))
	if err != nil {
		log.Fatalf("Error while initializing dqlite %v", err)
	}
	ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	if err := app.Ready(ctx); err != nil {
		log.Fatalf("Error while initializing dqlite %v", err)
	}
	db, _ := app.Open(context.Background(), "my-database")
	// db is a *sql.DB object
	if _, err := db.Exec(schema); err != nil {
		fmt.Println("Unable to create schema.")
	}

	time.Sleep(5 * time.Second)
	log.Printf("Continuing...")
	// we will launch this many go routines each, insert, query and delete
	for i := 0; i < 10; i++ {
		leader, _ := isLeader(ctx, *app)
		if leader {
			log.Printf("I am the leader")
		}
		go doInsert(context.Background(), db, fmt.Sprintf("runner-%d", i))
		//go doDelete(context.Background(), db, fmt.Sprintf("runner-%d", i))
		go doQuery(context.Background(), db, fmt.Sprintf("runner-%d", i))
	}

	for {
	}
}
