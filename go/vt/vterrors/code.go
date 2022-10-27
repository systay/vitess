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

package vterrors

import (
	"fmt"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	VT03001 = errorWithState("VT03001", vtrpcpb.Code_INVALID_ARGUMENT, SyntaxError, "Aggregate functions take a single argument '%s'", "The planner accepts aggregate functions that take a single argument only.")
	VT03002 = errorWithState("VT03002", vtrpcpb.Code_INVALID_ARGUMENT, ForbidSchemaChange, "Changing schema from '%s' to '%s' is not allowed", "This schema change is not allowed. You cannot change the keyspace of a table.")
	VT03003 = errorWithState("VT03003", vtrpcpb.Code_INVALID_ARGUMENT, UnknownTable, "Unknown table '%s' in MULTI DELETE", "The specified table in this DELETE statement is unknown.")
	VT03004 = errorWithState("VT03004", vtrpcpb.Code_INVALID_ARGUMENT, NonUpdateableTable, "The target table %s of the DELETE is not updatable", "You cannot delete something that is not a real MySQL table.")
	VT03005 = errorWithState("VT03005", vtrpcpb.Code_INVALID_ARGUMENT, WrongGroupField, "Can't group on '%s'", "The planner does not allow grouping on such field.")
	VT03006 = errorWithState("VT03006", vtrpcpb.Code_INVALID_ARGUMENT, WrongValueCountOnRow, "Column count doesn't match value count at row 1", "The number of columns you want to insert do not match the number of columns of your SELECT query.")
	VT03007 = errorWithoutState("VT03007", vtrpcpb.Code_INVALID_ARGUMENT, "Keyspace not specified", "You need to explicitly add a keyspace qualifier.")
	VT03008 = errorWithState("VT03008", vtrpcpb.Code_INVALID_ARGUMENT, CantUseOptionHere, "Incorrect usage/placement of '%s'", "The given token is not usable in this situation. Please refer to the MySQL documentation to learn more about your token's syntax.")
	VT03009 = errorWithState("VT03009", vtrpcpb.Code_INVALID_ARGUMENT, WrongValueForVar, "Unexpected value type for '%s': %v", "You cannot assign this type to the given variable.")
	VT03010 = errorWithState("VT03010", vtrpcpb.Code_INVALID_ARGUMENT, IncorrectGlobalLocalVar, "Variable '%s' is a read only variable", "You cannot set the given variable as it is a read-only variable.")
	VT03011 = errorWithoutState("VT03011", vtrpcpb.Code_INVALID_ARGUMENT, "Invalid value type: %v", "The given value type is not accepted.")
	VT03012 = errorWithoutState("VT03012", vtrpcpb.Code_INVALID_ARGUMENT, "Invalid syntax: %s", "The syntax is invalid. Please refer to the MySQL documentation to learn the proper syntax.")
	VT03013 = errorWithState("VT03013", vtrpcpb.Code_INVALID_ARGUMENT, NonUniqTable, "Not unique table/alias: '%s'", "This table or alias name is already use. Please use another one that is unique.")
	VT03014 = errorWithState("VT03014", vtrpcpb.Code_INVALID_ARGUMENT, BadFieldError, "Unknown column '%d' in '%s'", "The given column is unknown.")
	VT03015 = errorWithoutState("VT03015", vtrpcpb.Code_INVALID_ARGUMENT, "Column has duplicate set values: '%v'", "You cannot assign more than one value to the same vindex.")
	VT03016 = errorWithoutState("VT03016", vtrpcpb.Code_INVALID_ARGUMENT, "Unknown vindex column: '%s'", "The given column is unknown in the vindex table.")
	VT03017 = errorWithState("VT03017", vtrpcpb.Code_INVALID_ARGUMENT, SyntaxError, "where clause can only be of the type 'pos > <value>'", "This vstream where clause can only be a greater than filter.")
	VT03018 = errorWithoutState("VT03018", vtrpcpb.Code_INVALID_ARGUMENT, "NEXT used on a non-sequence table", "You cannot use the NEXT syntax on a table that is not a sequence table.")
	VT03019 = errorWithoutState("VT03019", vtrpcpb.Code_INVALID_ARGUMENT, "symbol %s not found", "The given symbol was not found or is not available.")
	VT03020 = errorWithoutState("VT03020", vtrpcpb.Code_INVALID_ARGUMENT, "symbol %s not found in subquery", "The given symbol was not found in the subquery.")
	VT03021 = errorWithoutState("VT03021", vtrpcpb.Code_INVALID_ARGUMENT, "ambiguous symbol reference: %v", "The given symbol is ambiguous. You can use a table qualifier to make unambiguous.")
	VT03022 = errorWithoutState("VT03022", vtrpcpb.Code_INVALID_ARGUMENT, "column %v not found in %v", "The given column cannot be found.")

	VT05001 = errorWithState("VT05001", vtrpcpb.Code_NOT_FOUND, DbDropExists, "Can't drop database '%s'; database doesn't exists", "The given database does not exist, Vitess cannot drop it.")
	VT05002 = errorWithState("VT05002", vtrpcpb.Code_NOT_FOUND, BadDb, "Can't alter database '%s'; unknown database", "The given database does not exist, Vitess cannot alter it.")
	VT05003 = errorWithState("VT05003", vtrpcpb.Code_NOT_FOUND, BadDb, "Unknown database '%s' in vschema", "The given database does not exist in the VSchema.")
	VT05004 = errorWithState("VT05004", vtrpcpb.Code_NOT_FOUND, UnknownTable, "Table '%s' does not exist", "The given table is unknown.")
	VT05005 = errorWithState("VT05005", vtrpcpb.Code_NOT_FOUND, NoSuchTable, "Table '%s' does not exist in keyspace '%s'", "The given table does not exist in this keyspace.")
	VT05006 = errorWithState("VT05006", vtrpcpb.Code_NOT_FOUND, UnknownSystemVariable, "Unknown system variable '%s'", "The given system variable is unknown.")
	VT05007 = errorWithoutState("VT05007", vtrpcpb.Code_NOT_FOUND, "No table info", "There are no available table information.")

	VT06001 = errorWithState("VT06001", vtrpcpb.Code_ALREADY_EXISTS, DbCreateExists, "Can't create database '%s'; database exists", "The given database name already exists, its creation is impossible.")

	VT09001 = errorWithState("VT09001", vtrpcpb.Code_FAILED_PRECONDITION, RequiresPrimaryKey, PrimaryVindexNotSet, "The table does not a primary vindex, the operation is impossible.")
	VT09002 = errorWithState("VT09002", vtrpcpb.Code_FAILED_PRECONDITION, InnodbReadOnly, "%s statement with a replica target", "This type of DML is not allowed on replica target.")
	VT09003 = errorWithoutState("VT09003", vtrpcpb.Code_FAILED_PRECONDITION, "Insert query does not have sharding column '%v' in the column list", "A sharding column is mandatory for the insert, please provide one.")
	VT09004 = errorWithoutState("VT09004", vtrpcpb.Code_FAILED_PRECONDITION, "Insert should contain column list or the table should have authoritative columns in vschema", "You need to provide the list of columns you want to insert, or provide an VSchema with authoritative columns. You can also turn on schema tracking to automatically have authoritative columns.")
	VT09005 = errorWithState("VT09005", vtrpcpb.Code_FAILED_PRECONDITION, NoDB, "No database selected: use keyspace<:shard><@type> or keyspace<[range]><@type> (<> are optional)", "A database must be selected.")
	VT09006 = errorWithoutState("VT09006", vtrpcpb.Code_FAILED_PRECONDITION, "%s VITESS_MIGRATION works only on primary tablet", "VITESS_MIGRATION works only on primary tablet.")
	VT09007 = errorWithoutState("VT09007", vtrpcpb.Code_FAILED_PRECONDITION, "%s VITESS_THROTTLED_APPS works only on primary tablet", "VITESS_THROTTLED_APPS works only on primary tablet.")

	VT12001 = errorWithoutState("VT12001", vtrpcpb.Code_UNIMPLEMENTED, "unsupported: %s", "This statement is unsupported by Vitess. Please use an alternative.")

	// VT13001 General Error
	VT13001 = errorWithoutState("VT13001", vtrpcpb.Code_INTERNAL, "[BUG] %s", "This error should not happen and is a bug. Please fill an issue on GitHub.")
	VT13002 = errorWithoutState("VT13002", vtrpcpb.Code_INTERNAL, "unexpected AST struct for query: %s", "This error should not happen and is a bug. Please fill an issue on GitHub.")

	VT14001 = errorWithoutState("VT14001", vtrpcpb.Code_UNAVAILABLE, "Connection error", "The connection failed.")
	VT14002 = errorWithoutState("VT14002", vtrpcpb.Code_UNAVAILABLE, "No available connection", "No available connection.")
	VT14003 = errorWithoutState("VT14003", vtrpcpb.Code_UNAVAILABLE, "No connection for tablet %v", "No connection for the given tablet.")

	VT17001 = errorWithoutState("VT17001", vtrpcpb.Code_CLUSTER_EVENT, "operation not allowed in state NOT_SERVING during query: %s", "The given operation can only be done in a non-NOT_SERVING state.")
	VT17002 = errorWithoutState("VT17002", vtrpcpb.Code_CLUSTER_EVENT, "invalid tablet type: REPLICA, want: PRIMARY", "A PRIMARY tablet is required for this operation.")

	Errors = []func(args ...any) *OurError{
		VT03001,
		VT03002,
		VT03003,
		VT03004,
		VT03005,
		VT03006,
		VT03007,
		VT03008,
		VT03009,
		VT03010,
		VT03011,
		VT03012,
		VT03013,
		VT03014,
		VT03015,
		VT03016,
		VT03017,
		VT03018,
		VT03019,
		VT03020,
		VT03021,
		VT03022,
		VT05001,
		VT05002,
		VT05003,
		VT05004,
		VT05005,
		VT05006,
		VT05007,
		VT06001,
		VT09001,
		VT09002,
		VT09003,
		VT09004,
		VT09005,
		VT09006,
		VT09007,
		VT12001,
		VT13001,
		VT13002,
		VT14001,
		VT14002,
		VT14003,
		VT17001,
		VT17002,
	}
)

type OurError struct {
	Err         error
	Description string
	ID          string
	State       State
}

func (o *OurError) Error() string {
	return o.Err.Error()
}

var _ error = (*OurError)(nil)

func errorWithoutState(id string, code vtrpcpb.Code, short, long string) func(args ...any) *OurError {
	return func(args ...any) *OurError {
		if len(args) != 0 {
			short = fmt.Sprintf(short, args...)
		}

		return &OurError{
			Err:         New(code, id+": "+short),
			Description: long,
			ID:          id,
		}
	}
}

func errorWithState(id string, code vtrpcpb.Code, state State, short, long string) func(args ...any) *OurError {
	return func(args ...any) *OurError {
		return &OurError{
			Err:         NewErrorf(code, state, id+": "+short, args...),
			Description: long,
			ID:          id,
			State:       state,
		}
	}
}