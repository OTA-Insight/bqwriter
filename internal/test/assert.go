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

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"testing"
)

func errorf(t *testing.T, contextArgs []interface{}, format string, args ...interface{}) {
	args = append(args, debug.Stack())
	format += "\n\nstack:\n>>>>>>>>>>>>>>>>>>>>>\n%s\n<<<<<<<<<<<<<<<<<<<<<\n"
	if len(contextArgs) > 0 {
		if contextFormat, ok := contextArgs[0].(string); ok {
			format = fmt.Sprintf(contextFormat, contextArgs[1:]...) + "\n" + format
		} else {
			format = fmt.Sprint(contextArgs...) + "\n" + format
		}
	}
	t.Errorf("\n"+format, args...)
}

// AssertError can be used to (test) assert if an error is non-nil,
// logging a message with context and stacktrace if false
func AssertError(t *testing.T, err error, contextArgs ...interface{}) bool {
	if err == nil {
		errorf(t, contextArgs, "no error returned while one is expected")
		return true
	}
	return false
}

// AssertIsError can be used to (test) assert if an error is a specific kind of error,
// logging a message with context and stacktrace if false
func AssertIsError(t *testing.T, err error, target error, contextArgs ...interface{}) bool {
	if !errors.Is(err, target) {
		errorf(t, contextArgs, "expected error %v (%T) to be %v (%T)", err, err, target, target)
		return false
	}
	return true
}

// AssertNoError can be used to (test) assert if an error is nil,
// logging a message with context and stacktrace if false
func AssertNoError(t *testing.T, err error, contextArgs ...interface{}) bool {
	if err != nil {
		errorf(t, contextArgs, "no error expected while one is returned: %v", err)
		return false
	}
	return true
}

// AssertIsNotError can be used to (test) assert if an error is not a specific kind of error,
// logging a message with context and stacktrace if false
func AssertIsNotError(t *testing.T, err error, target error, contextArgs ...interface{}) bool {
	if errors.Is(err, target) {
		errorf(t, contextArgs, "expected error %v (%T) to not be %v (%T)", err, err, target, target)
		return false
	}
	return true
}

// AssertNoErrorFatal can be used to (test) assert if an error is nil,
// logging a message with context and stacktrace if true, and halting the program afterwards.
func AssertNoErrorFatal(t *testing.T, err error, contextArgs ...interface{}) bool {
	if err != nil {
		t.Fatalf("no error expected while one is returned: %v", err)
		return false
	}
	return true
}

// AssertEqual can be used to (test) assert that 2 values are deep (reflect) equal,
// logging a message with context and stacktrace if false
func AssertEqual(t *testing.T, a, b interface{}, contextArgs ...interface{}) bool {
	if !reflect.DeepEqual(a, b) {
		errorf(t, contextArgs, "expected %v == %v", a, b)
		return false
	}
	return true
}

// AssertEqualAny can be used to (test) assert that a value is deep (reflect) equal with any of the given possibilities,
// logging a message with context and stacktrace if false
func AssertEqualAny(t *testing.T, a interface{}, possibilities []interface{}, contextArgs ...interface{}) bool {
	for _, b := range possibilities {
		if reflect.DeepEqual(a, b) {
			return true
		}
	}
	errorf(t, contextArgs, "expected %v equal to any of %v", a, possibilities)
	return true
}

// AssertNotEqual can be used to (test) assert that 2 values are not deep (reflect) equal,
// logging a message with context and stacktrace if false.
func AssertNotEqual(t *testing.T, a, b interface{}, contextArgs ...interface{}) bool {
	if reflect.DeepEqual(a, b) {
		errorf(t, contextArgs, "expected %v != %v", a, b)
		return false
	}
	return true
}

// AssertTrue can be used to (test) assert that a boolean is false
// logging a message with context and stacktrace if true.
func AssertTrue(t *testing.T, b bool, contextArgs ...interface{}) bool {
	if !b {
		errorf(t, contextArgs, "epected %v to be true", b)
		return false
	}
	return true
}

// AssertFalse can be used to (test) assert that a boolean is true
// logging a message with context and stacktrace if false.
func AssertFalse(t *testing.T, b bool, contextArgs ...interface{}) bool {
	if b {
		errorf(t, contextArgs, "epected %v to be false", b)
		return false
	}
	return true
}

// AssertBytesEqual can be used to (test) assert that two byte slices are equal
// logging a message with context and stacktrace if false.
func AssertBytesEqual(t *testing.T, a, b []byte, contextArgs ...interface{}) bool {
	if !bytes.Equal(a, b) {
		errorf(t, contextArgs, "expected %x == %x", a, b)
		return false
	}
	return true
}

// AssertBytesNotEqual can be used to (test) assert that two byte slices are not equal
// logging a message with context and stacktrace if false.
func AssertBytesNotEqual(t *testing.T, a, b []byte, contextArgs ...interface{}) bool {
	if bytes.Equal(a, b) {
		errorf(t, contextArgs, "expected %x != %x", a, b)
		return false
	}
	return true
}

// AssertNotEqualShallow can be used to (test) assert that two values are by value equal or by pointer,
// logging a message with context and stacktrace if false.
func AssertNotEqualShallow(t *testing.T, a, b interface{}, contextArgs ...interface{}) bool {
	if a == b {
		errorf(t, contextArgs, "expected (shallow) %v != %v", a, b)
		return false
	}
	return true
}

func isNil(a interface{}, contextArgs ...interface{}) bool {
	if a == nil {
		return true
	}
	switch reflect.TypeOf(a).Kind() {
	case reflect.Ptr, reflect.Map, reflect.Array, reflect.Chan, reflect.Slice:
		if reflect.ValueOf(a).IsNil() {
			return true
		}
	}
	return false
}

// AssertNil can be used to (test) assert that a value is nil,
// logging a message with context and stacktrace if false.
func AssertNil(t *testing.T, a interface{}, contextArgs ...interface{}) bool {
	if !isNil(a) {
		errorf(t, contextArgs, "expected %v to be nil", a)
		return false
	}
	return true
}

// AssertNotNil can be used to (test) assert that a value is not nil,
// logging a message with context and stacktrace if false.
func AssertNotNil(t *testing.T, a interface{}, contextArgs ...interface{}) bool {
	if isNil(a) {
		errorf(t, contextArgs, "expected %v not to be nil", a)
		return false
	}
	return true
}
