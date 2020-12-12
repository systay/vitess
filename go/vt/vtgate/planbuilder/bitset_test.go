package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	F0 bitSet = 1 << iota
	F1
	F2
	F3
	F4
	F5
	F6
	F7
)

var singleBits = []bitSet{F0, F1, F2, F3, F4, F5, F6, F7}

func TestProduceBitsets(t *testing.T) {
	dp := dpTableT{}
	for i := 0; i <= 255; i++ {
		solves := bitSet(i)
		dp[solves] = &routePlan{solved: solves}
	}
	bitSets := dp.bitSetsOfSize(1)
	equal(t, singleBits, bitSets)
}

func equal(t *testing.T, bss []bitSet, jt []joinTree) {
	t.Helper()
	require.Equal(t, len(bss), len(jt), "size different")

	for _, jp := range jt {
		// asserting this way because the order is not guaranteed
		assert.Contains(t, bss, jp.solves())
	}
}

func TestHas(t *testing.T) {
	assert.True(t, (F1 | F2).overlaps(F1|F2))
	assert.True(t, (F1 | F2).overlaps(F1))
	assert.True(t, (F1).overlaps(F1|F2))
	assert.False(t, (F1 | F2).overlaps(F3))
	assert.False(t, (F3).overlaps(F1|F2))
}
