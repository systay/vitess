/*
Copyright 2019 The Vitess Authors.

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

package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestGenerateLimitQueryForSimpleSelect(t *testing.T) {
	input := "select 42 from dual"
	stmt, err := sqlparser.Parse(input)
	require.NoError(t, err)

	plan := GenerateLimitQuery(stmt.(sqlparser.SelectStatement), true)
	assert.Equal(t, "select sql_calc_found_rows 42 from dual limit :#maxLimit", plan.Query)
	assert.Equal(t, input, sqlparser.String(stmt))
}

func TestGenerateLimitQueryForUnion(t *testing.T) {
	input := "select 42 from dual union select 12 from dual union select 43 from dual"
	stmt, err := sqlparser.Parse(input)
	require.NoError(t, err)

	plan := GenerateLimitQuery(stmt.(sqlparser.SelectStatement), true)
	assert.Equal(t, "select sql_calc_found_rows 42 from dual union select 12 from dual union select 43 from dual limit :#maxLimit", plan.Query)
	assert.Equal(t, input, sqlparser.String(stmt))
}
