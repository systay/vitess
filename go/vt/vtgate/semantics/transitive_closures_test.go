package semantics

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestTransitiveClosures_SimpleAddAndGet(t *testing.T) {
	// Create a new TransitiveClosures.
	tc := NewTransitiveClosures()

	// Basic expressions (pretend these are valid map keys).
	exprA := sqlparser.NewColName("colA")
	exprB := sqlparser.NewColName("colB")

	// Add an equality between exprA and exprB.
	tc.Add(exprA, exprB)

	// Now fetching either expression should return a slice containing both.
	gotA := tc.Get(exprA)
	gotB := tc.Get(exprB)

	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprB}, gotA, "exprA's set must have A and B")
	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprB}, gotB, "exprB's set must have A and B")
}

func TestTransitiveClosures_MultipleMerges(t *testing.T) {
	tc := NewTransitiveClosures()

	// We'll connect colA=colB, colB=colC, colX=colY, then merge colC=colX
	exprA := sqlparser.NewColName("colA")
	exprB := sqlparser.NewColName("colB")
	exprC := sqlparser.NewColName("colC")
	exprX := sqlparser.NewColName("colX")
	exprY := sqlparser.NewColName("colY")

	tc.Add(exprA, exprB) // A==B
	tc.Add(exprB, exprC) // B==C => A==B==C
	tc.Add(exprX, exprY) // X==Y

	// Now merge (C==X) => entire set A,B,C merges with X,Y
	tc.Add(exprC, exprX)

	// All of A,B,C,X,Y should be in the same set
	setA := tc.Get(exprA)
	setB := tc.Get(exprB)
	setX := tc.Get(exprX)
	expected := []sqlparser.Expr{exprA, exprB, exprC, exprX, exprY}

	assert.ElementsMatch(t, expected, setA, "A's set should contain A,B,C,X,Y")
	assert.ElementsMatch(t, expected, setB, "B's set should contain A,B,C,X,Y")
	assert.ElementsMatch(t, expected, setX, "X's set should contain A,B,C,X,Y")
}

func TestTransitiveClosures_MultipleMergesWithDifferentReferences(t *testing.T) {
	// Same test as above, but with different references for the same expressions.
	tc := NewTransitiveClosures()

	// We'll connect colA=colB, colB=colC, colX=colY, then merge colC=colX
	// exprA := sqlparser.NewColName("colA")
	// exprB := sqlparser.NewColName("colB")
	// exprC := sqlparser.NewColName("colC")
	// exprX := sqlparser.NewColName("colX")
	// exprY := sqlparser.NewColName("colY")

	tc.Add(sqlparser.NewColName("colA"), sqlparser.NewColName("colB")) // A==B
	tc.Add(sqlparser.NewColName("colB"), sqlparser.NewColName("colC")) // B==C => A==B==C
	tc.Add(sqlparser.NewColName("colX"), sqlparser.NewColName("colY")) // X==Y

	// Now merge (C==X) => entire set A,B,C merges with X,Y
	tc.Add(sqlparser.NewColName("colC"), sqlparser.NewColName("colX"))

	// All of A,B,C,X,Y should be in the same set
	setA := tc.Get(sqlparser.NewColName("colA"))
	setB := tc.Get(sqlparser.NewColName("colB"))
	setX := tc.Get(sqlparser.NewColName("colX"))
	expected := []sqlparser.Expr{
		sqlparser.NewColName("colA"),
		sqlparser.NewColName("colB"),
		sqlparser.NewColName("colC"),
		sqlparser.NewColName("colX"),
		sqlparser.NewColName("colY"),
	}

	assert.ElementsMatch(t, expected, setA, "A's set should contain A,B,C,X,Y")
	assert.ElementsMatch(t, expected, setB, "B's set should contain A,B,C,X,Y")
	assert.ElementsMatch(t, expected, setX, "X's set should contain A,B,C,X,Y")
}

func TestTransitiveClosures_NotMapKeys(t *testing.T) {
	tc := NewTransitiveClosures()

	// ValTuple is not a valid map key
	notMapKeyExpr := sqlparser.ValTuple{
		sqlparser.NewIntLiteral("1"),
		sqlparser.NewIntLiteral("2"),
	}

	// Another expression that might not be a valid map key
	notMapKeyExpr2 := sqlparser.ValTuple{
		sqlparser.NewIntLiteral("4"),
		sqlparser.NewIntLiteral("5"),
	}

	tc.Add(notMapKeyExpr, notMapKeyExpr2)

	// We expect them to be in the same equivalence set
	got1 := tc.Get(notMapKeyExpr)
	got2 := tc.Get(notMapKeyExpr2)

	assert.ElementsMatch(t, []sqlparser.Expr{notMapKeyExpr, notMapKeyExpr2}, got1)
	assert.ElementsMatch(t, []sqlparser.Expr{notMapKeyExpr, notMapKeyExpr2}, got2)
}

// func TestTransitiveClosures_MixedMapKeyValidity(t *testing.T) {
// 	tc := NewTransitiveClosures()
//
// 	// exprA is valid map key
// 	exprA := sqlparser.NewColName("colA")
//
// 	// exprFunc is not a valid map key
// 	exprFunc := &sqlparser.FuncExpr{Name: sqlparser.NewColIdent("SomeFunc")}
//
// 	tc.Add(exprA, exprFunc)
//
// 	// Both should be in the same set
// 	gotA := tc.Get(exprA)
// 	gotF := tc.Get(exprFunc)
//
// 	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprFunc}, gotA)
// 	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprFunc}, gotF)
// }

func TestTransitiveClosures_DuplicateAdds(t *testing.T) {
	tc := NewTransitiveClosures()

	exprA := sqlparser.NewColName("colA")
	exprB := sqlparser.NewColName("colB")

	tc.Add(exprA, exprB)
	tc.Add(exprA, exprB) // Duplicate Add should not cause problems.

	// They should remain in the same set with no duplicates in Get()
	got := tc.Get(exprA)
	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprB}, got)
}

// func TestTransitiveClosures_UpdateIDsForNotMapKeys(t *testing.T) {
// 	tc := NewTransitiveClosures()
//
// 	// This test specifically ensures setID updates properly for notMapKeys
// 	exprA := &sqlparser.FuncExpr{Name: sqlparser.NewColIdent("f1")}
// 	exprB := &sqlparser.FuncExpr{Name: sqlparser.NewColIdent("f2")}
// 	exprC := &sqlparser.FuncExpr{Name: sqlparser.NewColIdent("f3")}
//
// 	// Add (A == B), then unify B with C => should produce A==B==C
// 	tc.Add(exprA, exprB)
// 	tc.Add(exprB, exprC)
//
// 	gotA := tc.Get(exprA)
// 	gotB := tc.Get(exprB)
// 	gotC := tc.Get(exprC)
//
// 	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprB, exprC}, gotA)
// 	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprB, exprC}, gotB)
// 	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprB, exprC}, gotC)
// }

func TestTransitiveClosures_StructuralMatchInMapKeys(t *testing.T) {
	tc := NewTransitiveClosures()

	// Two colNames with the same name but different pointer references
	exprPtr1 := sqlparser.NewColName("same")
	exprPtr2 := sqlparser.NewColName("same")

	// Even though the AST pointers differ, structurally they are the same colName
	// If ValidAsMapKey(expr) => pointer-based map usage might skip exprPtr2.
	// The code attempts to do a structural match fallback in `Get()`.
	tc.Add(exprPtr1, exprPtr2)

	got1 := tc.Get(exprPtr1)
	got2 := tc.Get(exprPtr2)

	// Both sets should contain the same references, though how you unify them
	// depends on your map fallback logic. Minimally, we expect a single equivalence set.
	assert.ElementsMatch(t, got1, got2, "Structurally identical expressions should unify")
	assert.Len(t, got1, 2, "Should have exactly two expressions in the set")
}
