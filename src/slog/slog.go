package slog

import (
  "fmt"
  "log"
  "strings"
  "runtime"
 _"reflect"
  "path/filepath"
)

type LogLevel int
const (
	LOG_ERR		LogLevel = 0
	LOG_INFO 	LogLevel = 10
	LOG_DEBUG	LogLevel = 20
)

const currentLogLevel = LOG_INFO

func Log(logLevel LogLevel, formating string, args ...interface{}) {
	var logLevelString string
	switch logLevel {
	case LOG_ERR:
		logLevelString = "ERROR"
	case LOG_INFO:
		logLevelString = "INFO"
	case LOG_DEBUG:
		logLevelString = "DEBUG"
	default:
		logLevelString = "UNKNOWN"
	}

	if logLevel <= currentLogLevel {
		var funcname string
		pc, filename, line, ok := runtime.Caller(1)
		// fmt.Println(reflect.TypeOf(pc), reflect.ValueOf(pc))
		if ok {
			funcname = runtime.FuncForPC(pc).Name()       // main.(*MyStruct).foo
			funcname = filepath.Ext(funcname)             // .foo
			funcname = strings.TrimPrefix(funcname, ".")  // foo
	  
			filename = filepath.Base(filename)  // /full/path/basename.go => basename.go
			
			log.Printf("%s - %s:%d(%s): %s\n", logLevelString, filename, line, funcname, fmt.Sprintf(formating, args...))
		} else {
			log.Printf(formating, args...)
		}
	}
}