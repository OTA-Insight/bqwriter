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
	"reflect"
	"testing"
)

func AssertError(t *testing.T, err error) {
	if err == nil {
		t.Error("no error returned while one is expected")
	}
}

func AssertNoError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("no error expected while one is returned: %v", err)
	}
}

func AssertNoErrorFatal(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("no error expected while one is returned: %v", err)
	}
}

func AssertEqual(t *testing.T, a, b interface{}) {
	if !reflect.DeepEqual(a, b) {
		t.Errorf("expected %v == %v", a, b)
	}
}

func AssertNotEqualShallow(t *testing.T, a, b interface{}) {
	if a == b {
		t.Errorf("expected (shallow) %v != %v", a, b)
	}
}

func AssertNil(t *testing.T, a interface{}) {
	if a == nil {
		return
	}
	switch reflect.TypeOf(a).Kind() {
	case reflect.Ptr, reflect.Map, reflect.Array, reflect.Chan, reflect.Slice:
		if reflect.ValueOf(a).IsNil() {
			return
		}
	}
	t.Errorf("expected %v to be nil", a)
}
