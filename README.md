# About this repo

This repo is still under construction.

It is a playground about my toy-level implementations of distributed systems and related tools.

# Available Toys

## Raft Protocol

This is a basic implementation of the Raft protocol, which enables basic multi-node consensus in a cluster with an odd number of nodes.

### Code Location
`./src/raft/*`


## Simple Log

This is a simple wrap of go's `log` to provide a fine-grained logging mechanism.

Basic Usage is: `slog.Log(slog.LOG_LEVEL, formating, args ...)`

This currently just prints the log messages to stdout like this:

```
2020/12/20 15:44:30 ERROR - appendentries_thread.go:112(AppendEntriesProcessor): Raft[1] there is an error in sendAppendEntries to Raft[2], will return
```

|date|LOG_LEVEL|filename|line|funcion name|Log message|
|-|-|-|-|-|-|
|2020/12/20 15:44:30|ERROR|appendentries_thread.go|:112|(AppendEntriesProcessor):|Raft[1] there is an error in sendAppendEntries to Raft[2], will return|

# Under construction

- [ ] Simple K/V store on Raft
- [ ] Distributed in-memory Cache