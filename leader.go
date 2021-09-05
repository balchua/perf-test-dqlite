package main

import (
	"context"
	"log"
	"net"
	"strings"

	"github.com/canonical/go-dqlite/app"
)

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
