package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"strings"
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

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("TIMETRACKING: %s took %s", name, elapsed)
}

func query(ctx context.Context, db *sql.DB, runnerId string) {
	d := time.Now().Add(10 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer timeTrack(time.Now(), "query")
	defer cancel()
	row := db.QueryRow(queryStatement)
	var result string
	if err := row.Scan(&result); err != nil {
		result = fmt.Sprintf("Error: %s", err.Error())
	}
	//Check context for error, If ctx.Err() != nil gracefully exit the current execution
	if ctx.Err() != nil {
		log.Printf("query - %s \n", ctx.Err())
	}
	log.Printf("%s - Retrieved successfully %s.\n", runnerId, result)
}

func insert(ctx context.Context, db *sql.DB, runnerId string) {
	d := time.Now().Add(100 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer timeTrack(time.Now(), "insert")
	defer cancel()
	now := time.Now()
	nanos := now.UnixNano()
	log.Printf("%s - Begin transaction.\n", runnerId)
	// t, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	// if err != nil {
	// 	fmt.Sprintf("Error: %s", err.Error())
	// }
	if _, err := db.Exec(update, nanos, "anyvalue"); err != nil {
		fmt.Sprintf("Error: %s", err.Error())
	}
	//Check context for error, If ctx.Err() != nil gracefully exit the current execution
	if ctx.Err() != nil {
		log.Printf("insert - %s \n", ctx.Err())
	}
	//t.Commit()
	log.Printf("%s - Inserted and committed successfully.\n", runnerId)

}

func delete(ctx context.Context, db *sql.DB, runnerId string) {
	d := time.Now().Add(100 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer timeTrack(time.Now(), "delete")
	defer cancel()
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
	//Check context for error, If ctx.Err() != nil gracefully exit the current execution
	if ctx.Err() != nil {
		log.Printf("delete - %s \n", ctx.Err())
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

	ticker := time.NewTicker(20 * time.Millisecond)
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
	fmt.Println("Continuing with insert")

	for i := 0; i < 100; i++ {
		leader, _ := isLeader(ctx, *app)
		if leader {
			log.Printf("I am the leader")
		}
		go doInsert(context.Background(), db, fmt.Sprintf("runner-%d", i))
		go doDelete(context.Background(), db, fmt.Sprintf("runner-%d", i))
		go doQuery(context.Background(), db, fmt.Sprintf("runner-%d", i))
	}

	for {
	}
}
