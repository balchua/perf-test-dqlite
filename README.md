# Perf test dqlite

This is to test how much hammering can dqlite take.

## Build

```shell
snapcraft --use-lxd
```

## Run


## Notes

Grep for the `TIMETRACKING` log, to see how long insert, query or delete is taking.  Most if not all deadlines are set to `100 ms`
