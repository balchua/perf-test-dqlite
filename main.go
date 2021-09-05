package main

import (
	"context"
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

func main() {
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
	db.SetMaxOpenConns(500)
	db.SetConnMaxIdleTime(10 * time.Second)
	db.SetMaxIdleConns(500)
	// db is a *sql.DB object
	if _, err := db.Exec(schema); err != nil {
		fmt.Println("Unable to create schema.")
	}

	time.Sleep(5 * time.Second)
	log.Printf("Continuing...")
	leader, _ := isLeader(ctx, *app)
	if leader {
		log.Printf("I am the leader")
	}

	// we will launch this many go routines each, insert, query and delete
	for i := 0; i < 1000; i++ {
		go doInsert(context.Background(), db, fmt.Sprintf("runner-%d", i))
		//go doDelete(context.Background(), db, fmt.Sprintf("runner-%d", i))
		go doQuery(context.Background(), db, fmt.Sprintf("runner-%d", i))
	}

	for {
	}
}
