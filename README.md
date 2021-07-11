# Perf test dqlite

This is to test how much hammering can dqlite take.

## Build

```shell
export CGO_LDFLAGS_ALLOW="-Wl,-z,now"
go build -tags libsqlite3
```

## Run

```shell
./perf-dqlite
```

## Notes

When the number of go-routines is increased to say 20, each performing an insert at every 100 milliseconds.
This can cause the program to core dump with this error.

```
perf-dqlite: src/db.c:40: db__open_follower: Assertion `db->follower == NULL' failed.
```
