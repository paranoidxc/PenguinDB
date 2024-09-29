package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
)

type Level int

type Settings struct {
	Path           string
	Name           string
	Ext            string
	TimeFORMAT     string
	OutputTerminal bool
}

var (
	DefaultPrefix      = ""
	DefaultCallerDepth = 2

	logger     *log.Logger
	isDebug    = false
	levelFlags = []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
)

const (
	DEBUG Level = iota
	INFO
	WARNING
	ERROR
	FATAL
)

// Setup initialize the log instance
func Setup(setting *Settings) {
	logger = log.New(os.Stdout, DefaultPrefix, log.Ldate|log.Ltime|log.Lmicroseconds|log.Lmsgprefix)

	if _, err := os.Stat(setting.Path); os.IsNotExist(err) {
		if err = os.MkdirAll(setting.Path, os.ModePerm); err != nil {
			return
		}
	}

	file, err := os.OpenFile(fmt.Sprintf("%s/%s.%s", setting.Path, setting.Name, setting.Ext), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	writers := []io.Writer{file}
	if setting.OutputTerminal {
		writers = append(writers, os.Stdout)
	}

	multiWriter := io.MultiWriter(writers...)
	if err == nil {
		logger.SetOutput(multiWriter)
	}
}

func SetDebugMode(debug bool) {
	isDebug = debug
}

// Debug output logs at debug level
func Debug(v ...interface{}) {
	if !isDebug {
		return
	}
	setPrefix(DEBUG)
	logger.Println(v...)
}

// Info output logs at info level
func Info(v ...interface{}) {
	setPrefix(INFO)
	logger.Println(v...)
}

// Warn output logs at warn level
func Warn(v ...interface{}) {
	setPrefix(WARNING)
	logger.Println(v...)
}

// Error output logs at error level
func Error(v ...interface{}) {
	setPrefix(ERROR)
	logger.Println(v...)
}

// Fatal output logs at fatal level
func Fatal(v ...interface{}) {
	setPrefix(FATAL)
	logger.Fatalln(v...)
}

// setPrefix set the prefix of the log output
func setPrefix(level Level) {
	var logPrefix string
	_, file, line, ok := runtime.Caller(DefaultCallerDepth)
	if ok {
		logPrefix = fmt.Sprintf("%s - %s:%d ", levelFlags[level], filepath.Base(file), line)
	} else {
		logPrefix = fmt.Sprintf("%s ", levelFlags[level])
	}

	logger.SetPrefix(logPrefix)
}
