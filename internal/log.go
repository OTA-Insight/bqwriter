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

package internal

import (
	"fmt"
	"os"
)

// Logger is the builtin implementation of the Logger interface,
// and logs error messages to the STDERR, but ignores debug messages.
type Logger struct{}

// Debug implements Logger::Debug
func (stdl Logger) Debug(args ...interface{}) {} // debug messages are ignored by default

// Debugf implements Logger::Debugf
func (stdl Logger) Debugf(template string, args ...interface{}) {} // debug messages are ignored by default

// Error implements Logger::Error
func (stdl Logger) Error(args ...interface{}) {
	fmt.Fprint(os.Stderr, args...)
}

// Errorf implements Logger::Errorf
func (stdl Logger) Errorf(template string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, template, args...)
}
