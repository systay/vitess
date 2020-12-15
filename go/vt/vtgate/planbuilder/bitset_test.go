/*
Copyright 2020 The Vitess Authors.

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

	"vitess.io/vitess/go/vt/vtgate/semantics"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	F0 semantics.TableSet = 1 << iota
	F1
	F2
	F3
	F4
	F5
	F6
	F7
)

var singleBits = []semantics.TableSet{F0, F1, F2, F3, F4, F5, F6, F7}

func TestProduceBitsets(t *testing.T) {
	dp := dpTableT{}
	for i := 1; i < 255; i++ {
		solves := semantics.TableSet(i)
		dp[solves] = &routePlan{solved: solves}
	}
	bitSets := dp.bitSetsOfSize(1)
	equal(t, singleBits, bitSets)
}

func equal(t *testing.T, bss []semantics.TableSet, jt []joinTree) {
	t.Helper()
	require.Equal(t, len(bss), len(jt), "size different")

	for _, jp := range jt {
		// asserting this way because the order is not guaranteed
		assert.Contains(t, bss, jp.solves())
	}
}
