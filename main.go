package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/client"
)

const (
	schema          = "CREATE TABLE IF NOT EXISTS model (key NUMBER, value TEXT, UNIQUE(key))"
	queryStatement  = "SELECT count(key) FROM model"
	update          = "INSERT OR REPLACE INTO model(key, value) VALUES(?, ?)"
	deleteStatement = "DELETE from model where key < ?"
)

func query(ctx context.Context, db *sql.DB, runnerId string) {
	row := db.QueryRow(queryStatement)
	var result string
	if err := row.Scan(&result); err != nil {
		result = fmt.Sprintf("Error: %s", err.Error())
	}
	log.Printf("%s - Retrieved successfully %s.\n", runnerId, result)
}

func insert(ctx context.Context, db *sql.DB, runnerId string) {
	now := time.Now()
	nanos := now.UnixNano()

	if _, err := db.Exec(update, nanos, "anyvalue"); err != nil {
		fmt.Sprintf("Error: %s", err.Error())
	}
	log.Printf("%s - Inserted successfully.\n", runnerId)
}

func delete(ctx context.Context, db *sql.DB, runnerId string) {
	now := time.Now()
	nanos := now.UnixNano()
	var result sql.Result
	var err error
	var rowsAffected int64
	if result, err = db.Exec(deleteStatement, nanos); err != nil {
		fmt.Sprintf("Error: %s", err.Error())
	}
	if result != nil {
		rowsAffected, err = result.RowsAffected()
		if err != nil {
			log.Fatal("Unable to perform delete.")
		}
		log.Printf("%s - Deleted successfully [%d].\n", runnerId, rowsAffected)
	}

}

func doInsert(ctx context.Context, db *sql.DB, runnerId string) {
	d := time.Now().Add(200 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			insert(ctx, db, runnerId)
		}
	}
}

func doDelete(ctx context.Context, db *sql.DB, runnerId string) {
	d := time.Now().Add(100 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()
	ticker := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-ticker.C:
			delete(ctx, db, runnerId)
		}
	}
}

func doQuery(ctx context.Context, db *sql.DB, runnerId string) {
	d := time.Now().Add(100 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			query(ctx, db, runnerId)
		}
	}
}

func main() {
	fmt.Println("Start")
	dir := "/tmp/node1"
	address := "127.0.0.1:6666" // Unique node address

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

	db, err := app.Open(context.Background(), "my-database")
	// db is a *sql.DB object
	if _, err := db.Exec(schema); err != nil {
		fmt.Println("Unable to create schema.")
	}
	fmt.Println("Continuing with insert")

	for i := 0; i < 10; i++ {
		go doInsert(context.Background(), db, fmt.Sprintf("runner-%d", i))
		go doDelete(context.Background(), db, fmt.Sprintf("runner-%d", i))
		go doQuery(context.Background(), db, fmt.Sprintf("runner-%d", i))
	}

	for {
	}
}
