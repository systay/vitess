/*
Copyright 2022 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package semantics

import (
	"fmt"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var UseIDs = false

type (
	ErrorCode int

	Error struct {
		Code   ErrorCode
		format string
		args   []any
	}

	info struct {
		format string
		state  vterrors.State
		code   vtrpcpb.Code
		id     string
	}
)

func NewError(code ErrorCode, args ...any) error {
	return &Error{
		Code: code,
		args: args,
	}
}

func NewErrorWithMessage(code ErrorCode, format string, args ...any) error {
	return &Error{
		Code:   code,
		format: format,
		args:   args,
	}
}

var errors = map[ErrorCode]info{
	UnionColumnsDoNotMatch: {
		format: "The used SELECT statements have a different number of columns",
		state:  vterrors.WrongNumberOfColumnsInSelect,
		code:   vtrpcpb.Code_FAILED_PRECONDITION,
	},
	Unsupported: {
		format: "This statement is unsupported by Vitess. Please rewrite your query to use supported syntax.",
		code:   vtrpcpb.Code_UNIMPLEMENTED,
		id:     "VT12001",
	},
}

const (
	UnionColumnsDoNotMatch ErrorCode = iota
	Unsupported
)

func (n *Error) Error() string {
	var format string
	if n.format != "" {
		format = n.format
	} else {
		f, ok := errors[n.Code]
		if !ok {
			return "unknown error"
		}
		format = f.format
		if UseIDs && f.id != "" {
			format = fmt.Sprintf("%s: %s", f.id, format)
		}
	}

	sprintf := fmt.Sprintf(format, n.args...)
	return sprintf
}

func (n *Error) ErrorState() vterrors.State {
	f, ok := errors[n.Code]
	if !ok {
		return vterrors.Undefined
	}

	return f.state
}

func (n *Error) ErrorCode() vtrpcpb.Code {
	f, ok := errors[n.Code]
	if !ok {
		return vtrpcpb.Code_UNKNOWN
	}

	return f.code
}
