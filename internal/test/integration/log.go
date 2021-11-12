// Copyright 2021 OTA Insight Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"os"
)

// Logger is the builtin implementation of the Logger interface,
// and logs error messages to the STDERR, but ignores debug messages.
type Logger struct {
	ShowDebug bool
}

// NewLogger creates a new Logger
func NewLogger(showDebug bool) *Logger {
	return &Logger{
		ShowDebug: showDebug,
	}
}

// Debug implements Logger::Debug
func (stdl *Logger) Debug(args ...interface{}) {
	if stdl.ShowDebug {
		args = append([]interface{}{"[DEBUG]"}, args...)
		fmt.Fprintln(os.Stderr, args...)
	}
}

// Debugf implements Logger::Debugf
func (stdl *Logger) Debugf(template string, args ...interface{}) {
	if stdl.ShowDebug {
		fmt.Fprintf(os.Stderr, "[DEBUG] "+template+"\n", args...)
	}
}

// Info logs info statements to the STDERR
func (stdl *Logger) Info(args ...interface{}) {
	args = append([]interface{}{"[INFO]"}, args...)
	fmt.Fprintln(os.Stderr, args...)
}

// Infof logs formatted info statements to the STDERR
func (stdl *Logger) Infof(template string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "[INFO] "+template+"\n", args...)
}

// Error implements Logger::Error
func (stdl *Logger) Error(args ...interface{}) {
	args = append([]interface{}{"[ERROR]"}, args...)
	fmt.Fprintln(os.Stderr, args...)
}

// Errorf implements Logger::Errorf
func (stdl *Logger) Errorf(template string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "[ERROR] "+template+"\n", args...)
}
