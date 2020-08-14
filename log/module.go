package log

import (
	"github.com/neuronlabs/neuron/log"
)

var logger = log.NewModuleLogger("postgres")

// Log levels constants.
const (
	LevelDebug3   = log.LevelDebug3
	LevelDebug2   = log.LevelDebug2
	LevelDebug    = log.LevelDebug
	LevelInfo     = log.LevelInfo
	LevelWarning  = log.LevelWarning
	LevelError    = log.LevelError
	LevelCritical = log.LevelCritical
)

var (
	// SetLevel is the set level function
	SetLevel = logger.SetLevel
	// Level is the current module log level.
	Level = logger.Level
	// Debug3f writes the formatted debug log.
	Debug3f = logger.Debug3f
	// Debug2f writes the formatted debug log.
	Debug2f = logger.Debug2f
	// Debugf writes the formatted debug log.
	Debugf = logger.Debugf
	// Infof writes the formatted info log.
	Infof = logger.Infof
	// Warningf writes the formatted warning log.
	Warningf = logger.Warningf
	// Errorf writes the formatted error log.
	Errorf = logger.Errorf
	// Fatalf writes the formatted fatal log.
	Fatalf = logger.Fatalf
	// Panicf writes the formatted panic log.
	Panicf = logger.Panicf
	// Debug3 writes the debug3 level logs.
	Debug3 = logger.Debug3
	// Debug2 writes the debug2 level logs.
	Debug2 = logger.Debug2
	// Debug writes the  debug log.
	Debug = logger.Debug
	// Info writes the info log.
	Info = logger.Info
	// Warning writes the warning log.
	Warning = logger.Warning
	// Error writes the error log.
	Error = logger.Error
	// Fatal writes the fatal log.
	Fatal = logger.Fatal
	// Panic writes the panic log.
	Panic = logger.Panic
)
