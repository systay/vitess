/*
Copyright 2025 The Vitess Authors.

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

import "vitess.io/vitess/go/vt/sqlparser"

// TransitiveClosures expression equality, so we can rewrite expressions efficiently
// It allows for transitive closures (e.g., if a == b and b == c, then a == c).
type TransitiveClosures struct {
	// ExprIDs stores the IDs of the expressions.
	// The ID can then be used to get other equivalent expressions from the Equalities slice
	exprIDs map[sqlparser.Expr]int

	notMapKeys []ExprID

	equalities [][]sqlparser.Expr
}

type ExprID struct {
	Expr sqlparser.Expr
	ID   int
}

// NewTransitiveClosures creates a new TransitiveClosures
func NewTransitiveClosures() *TransitiveClosures {
	return &TransitiveClosures{
		exprIDs: make(map[sqlparser.Expr]int),
	}
}

func (ce *TransitiveClosures) getID(e sqlparser.Expr) (int, bool) {
	if !ValidAsMapKey(e) {
		for _, id := range ce.notMapKeys {
			if sqlparser.Equals.Expr(id.Expr, e) {
				return id.ID, true
			}
		}

		return 0, false
	}

	// first let's try to find the expression in the map
	id, found := ce.exprIDs[e]
	if found {
		return id, found
	}

	// we might still have a match in the map, just not by reference
	for expr, id := range ce.exprIDs {
		if sqlparser.Equals.Expr(expr, e) {
			defer func() {
				// we do this in the defer so we don't mutate the map while iterating over it
				ce.exprIDs[e] = id
			}()
			return id, true
		}
	}

	return 0, false
}

func (ce *TransitiveClosures) setID(e sqlparser.Expr, id int) {
	if ValidAsMapKey(e) {
		ce.exprIDs[e] = id
		return
	}

	for idx, notMap := range ce.notMapKeys {
		if sqlparser.Equals.Expr(notMap.Expr, e) {
			ce.notMapKeys[idx].ID = id
			return
		}
	}
	ce.notMapKeys = append(ce.notMapKeys, ExprID{Expr: e, ID: id})
}

// Add adds a new equality to the TransitiveClosures
func (ce *TransitiveClosures) Add(lhs, rhs sqlparser.Expr) {
	lhsID, lok := ce.getID(lhs)
	rhsID, rok := ce.getID(rhs)
	if !lok && !rok {
		// neither expression is known
		id := len(ce.equalities)
		ce.equalities = append(ce.equalities, []sqlparser.Expr{lhs, rhs})
		ce.setID(lhs, id)
		ce.setID(rhs, id)
		return
	}

	if !lok {
		// lhs is not known
		ce.equalities[rhsID] = append(ce.equalities[rhsID], lhs)
		ce.setID(lhs, rhsID)
		return
	}

	if !rok {
		// rhs is not known
		ce.equalities[lhsID] = append(ce.equalities[lhsID], rhs)
		ce.setID(rhs, lhsID)
	}

	// merge the two sets
	if lhsID != rhsID {
		ce.equalities[lhsID] = append(ce.equalities[lhsID], ce.equalities[rhsID]...)
		for _, expr := range ce.equalities[rhsID] {
			ce.setID(expr, lhsID)
		}
		// we don't want to shuffle any elements around, so we just set the rhsID to nil
		ce.equalities[rhsID] = nil
	}
}

// Get returns all expressions that are equal to the given expression
func (ce *TransitiveClosures) Get(expr sqlparser.Expr) []sqlparser.Expr {
	id, found := ce.getID(expr)
	if !found {
		return []sqlparser.Expr{expr}
	}

	return ce.equalities[id]
}
