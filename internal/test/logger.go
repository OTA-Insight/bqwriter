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

package test

// Logger is a `bqwriter/log.Logger` implementation for (unit) testing purposes only.
type Logger struct{}

// Debug implements bqwriter/log.Logger.Debug
func (Logger) Debug(_args ...interface{}) {}

// Debugf implements bqwriter/log.Logger.Debugf
func (Logger) Debugf(_template string, _args ...interface{}) {}

// Error implements bqwriter/log.Logger.Error
func (Logger) Error(_args ...interface{}) {}

// Errorf implements bqwriter/log.Logger.Errorf
func (Logger) Errorf(_template string, _args ...interface{}) {}
