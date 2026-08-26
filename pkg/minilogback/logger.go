package minilogback

import (
	"bytes"
	"fmt"
)

type Logger struct {
	appender *Appender
	name     string
}

func (l *Logger) Debug(message string, fields ...Field) PublishResult {
	return l.appender.log(l.name, DebugLevel, message, fields)
}

func (l *Logger) Info(message string, fields ...Field) PublishResult {
	return l.appender.log(l.name, InfoLevel, message, fields)
}

func (l *Logger) Warn(message string, fields ...Field) PublishResult {
	return l.appender.log(l.name, WarnLevel, message, fields)
}

func (l *Logger) Error(message string, fields ...Field) PublishResult {
	return l.appender.log(l.name, ErrorLevel, message, fields)
}

func (l *Logger) Debugf(format string, args ...any) PublishResult {
	return l.appender.log(l.name, DebugLevel, fmt.Sprintf(format, args...), nil)
}

func (l *Logger) Infof(format string, args ...any) PublishResult {
	return l.appender.log(l.name, InfoLevel, fmt.Sprintf(format, args...), nil)
}

func (l *Logger) Warnf(format string, args ...any) PublishResult {
	return l.appender.log(l.name, WarnLevel, fmt.Sprintf(format, args...), nil)
}

func (l *Logger) Errorf(format string, args ...any) PublishResult {
	return l.appender.log(l.name, ErrorLevel, fmt.Sprintf(format, args...), nil)
}

// Writer returns an io.Writer suitable for standard log.Logger integration.
func (a *Appender) Writer(level Level) *LogWriter { return &LogWriter{appender: a, level: level} }

type LogWriter struct {
	appender *Appender
	level    Level
}

func (w *LogWriter) Write(data []byte) (int, error) {
	message := string(bytes.TrimSuffix(data, []byte{'\n'}))
	result := w.appender.Log(w.level, message)
	if result != Accepted && result != Filtered {
		return 0, fmt.Errorf("minilogback publish: %s", result)
	}
	return len(data), nil
}
