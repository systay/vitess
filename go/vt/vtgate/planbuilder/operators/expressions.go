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

package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// BreakExpressionInLHSandRHS takes an expression and
// extracts the parts that are coming from one of the sides into `ColName`s that are needed
func BreakExpressionInLHSandRHS(
	ctx *plancontext.PlanningContext,
	expr sqlparser.Expr,
	lhs semantics.TableSet,
) (col JoinColumn, err error) {
	extract := func(deps semantics.TableSet) bool {
		return deps.IsSolvedBy(lhs)
	}

	return extractColNamesFrom(ctx, expr, extract)
}

// ExtractExpForTable takes an expression and extracts any ColName:s that are
// coming from the table identified by the provided TableSet.
// All these ColNames will be replaced with arguments.
// The resulting JoinColumn contains the information needed to gather the necessary
// arguments to evaluate the remaining expression.
func ExtractExpForTable(
	ctx *plancontext.PlanningContext,
	expr sqlparser.Expr,
	tableToKeep semantics.TableSet,
) (col JoinColumn, err error) {
	extract := func(deps semantics.TableSet) bool {
		return !deps.IsSolvedBy(tableToKeep)
	}

	return extractColNamesFrom(ctx, expr, extract)
}

func extractColNamesFrom(ctx *plancontext.PlanningContext, expr sqlparser.Expr, extract func(semantics.TableSet) bool) (col JoinColumn, err error) {
	rewrittenExpr := sqlparser.CopyOnRewrite(expr, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
		nodeExpr := shouldExtract(cursor.Node())
		if nodeExpr == nil {
			return
		}
		if !extract(ctx.SemTable.RecursiveDeps(nodeExpr)) {
			return
		}

		bvName := ctx.GetReservedArgumentFor(nodeExpr)
		col.LHSExprs = append(col.LHSExprs, BindVarExpr{
			Name: bvName,
			Expr: nodeExpr,
		})
		arg := sqlparser.NewArgument(bvName)
		// we are replacing one of the sides of the comparison with an argument,
		// but we don't want to lose the type information we have, so we copy it over
		ctx.SemTable.CopyExprInfo(nodeExpr, arg)
		cursor.Replace(arg)
	}, nil).(sqlparser.Expr)

	if err != nil {
		return JoinColumn{}, err
	}
	ctx.JoinPredicates[expr] = append(ctx.JoinPredicates[expr], rewrittenExpr)
	col.RHSExpr = rewrittenExpr
	return
}

func shouldExtract(node sqlparser.SQLNode) sqlparser.Expr {
	switch node.(type) {
	case *sqlparser.ColName, sqlparser.AggrFunc:
		return node.(sqlparser.Expr)
	default:
		return nil
	}
}
