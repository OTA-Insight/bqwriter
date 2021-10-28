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

package bqwriter

import "errors"

// internal errors, as to not lock them in as part of the API,
// given these are errors not meant to be caught by users but really indicating a bug
var (
	// invalidParamErr is returned in case we exit a function early with an error,
	// due to an invalid parameter passed in by the callee.
	invalidParamErr = errors.New("invalid function parameter")

	// notSupportedErr is returned in case a feature is used that is not (yet) supported.
	notSupportedErr = errors.New("not supported")
)
