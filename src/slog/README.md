# About slog

slog is short for Simple Logging. It is a simple wrap of go's `log` to provide a fine-grained logging mechanism.

Basic Usage is: `slog.Log(slog.LOG_LEVEL, formating, args ...)`

# LOG_LEVEL

The LOG_LEVEL is a enum-liked int value.

All messages with a lower LOG_LEVEL value will be recorded to the output.

Basiclly, `LOG_ERR` < `LOG_INFO` < `LOG_DEBUG`. 

# Composition of log message

```
2020/12/20 15:44:30 ERROR - appendentries_thread.go:112(AppendEntriesProcessor): Raft[1] there is an error in sendAppendEntries to Raft[2], will return
```

|date|LOG_LEVEL|filename|line|funcion name|Log message|
|-|-|-|-|-|-|
|2020/12/20 15:44:30|ERROR|appendentries_thread.go|:112|(AppendEntriesProcessor):|Raft[1] there is an error in sendAppendEntries to Raft[2], will return|

# TODO
- [ ] Specifies a specific log file that log messages should be redirected to
- [ ] A convenient way to set LOG_LEVEL
- [ ] LOG_LEVEL per service