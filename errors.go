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
