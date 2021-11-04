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
	"reflect"
	"runtime/debug"
	"testing"
)

func errorf(t *testing.T, format string, args ...interface{}) {
	args = append(args, debug.Stack())
	t.Errorf(format+"\n\nstack:\n>>>>>>>>>>>>>>>>>>>>>\n%s\n<<<<<<<<<<<<<<<<<<<<<\n", args...)
}

func AssertError(t *testing.T, err error) bool {
	if err == nil {
		errorf(t, "no error returned while one is expected")
		return true
	}
	return false
}

func AssertIsError(t *testing.T, err error, target error) bool {
	if !errors.Is(err, target) {
		errorf(t, "expected error %v (%T) to be %v (%T)", err, err, target, target)
		return false
	}
	return true
}

func AssertNoError(t *testing.T, err error) bool {
	if err != nil {
		errorf(t, "no error expected while one is returned: %v", err)
		return false
	}
	return true
}

func AssertIsNotError(t *testing.T, err error, target error) bool {
	if errors.Is(err, target) {
		errorf(t, "expected error %v (%T) to not be %v (%T)", err, err, target, target)
		return false
	}
	return true
}

func AssertNoErrorFatal(t *testing.T, err error) bool {
	if err != nil {
		t.Fatalf("no error expected while one is returned: %v", err)
		return false
	}
	return true
}

func AssertEqual(t *testing.T, a, b interface{}) bool {
	if !reflect.DeepEqual(a, b) {
		errorf(t, "expected %v == %v", a, b)
		return false
	}
	return true
}

func AssertNotEqual(t *testing.T, a, b interface{}) bool {
	if reflect.DeepEqual(a, b) {
		errorf(t, "expected %v != %v", a, b)
		return false
	}
	return true
}

func AssertTrue(t *testing.T, b bool) bool {
	if !b {
		errorf(t, "epected %v to be true", b)
		return false
	}
	return true
}

func AssertFalse(t *testing.T, b bool) bool {
	if b {
		errorf(t, "epected %v to be false", b)
		return false
	}
	return true
}

func AssertBytesEqual(t *testing.T, a, b []byte) bool {
	if !bytes.Equal(a, b) {
		errorf(t, "expected %x == %x", a, b)
		return false
	}
	return true
}

func AssertBytesNotEqual(t *testing.T, a, b []byte) bool {
	if bytes.Equal(a, b) {
		errorf(t, "expected %x != %x", a, b)
		return false
	}
	return true
}

func AssertNotEqualShallow(t *testing.T, a, b interface{}) bool {
	if a == b {
		errorf(t, "expected (shallow) %v != %v", a, b)
		return false
	}
	return true
}

func isNil(a interface{}) bool {
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

func AssertNil(t *testing.T, a interface{}) bool {
	if !isNil(a) {
		errorf(t, "expected %v to be nil", a)
		return false
	}
	return true
}

func AssertNotNil(t *testing.T, a interface{}) bool {
	if isNil(a) {
		errorf(t, "expected %v not to be nil", a)
		return false
	}
	return true
}
