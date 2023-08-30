/*
Copyright 2021 The Vitess Authors.

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
// Code generated by Sizegen. DO NOT EDIT.

package engine

import (
	"math"
	"reflect"
	"unsafe"

	hack "vitess.io/vitess/go/hack"
)

type cachedObject interface {
	CachedSize(alloc bool) int64
}

func (cached *AggregateParams) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(96)
	}
	// field Alias string
	size += hack.RuntimeAllocSize(int64(len(cached.Alias)))
	// field Expr vitess.io/vitess/go/vt/sqlparser.Expr
	if cc, ok := cached.Expr.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field Original *vitess.io/vitess/go/vt/sqlparser.AliasedExpr
	size += cached.Original.CachedSize(true)
	return size
}
func (cached *AlterVSchema) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(16)
	}
	// field Keyspace *vitess.io/vitess/go/vt/vtgate/vindexes.Keyspace
	size += cached.Keyspace.CachedSize(true)
	// field AlterVschemaDDL *vitess.io/vitess/go/vt/sqlparser.AlterVschema
	size += cached.AlterVschemaDDL.CachedSize(true)
	return size
}
func (cached *CheckCol) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(24)
	}
	// field WsCol *int
	size += hack.RuntimeAllocSize(int64(8))
	return size
}

//go:nocheckptr
func (cached *Concatenate) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(32)
	}
	// field Sources []vitess.io/vitess/go/vt/vtgate/engine.Primitive
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Sources)) * int64(16))
		for _, elem := range cached.Sources {
			if cc, ok := elem.(cachedObject); ok {
				size += cc.CachedSize(true)
			}
		}
	}
	// field NoNeedToTypeCheck map[int]any
	if cached.NoNeedToTypeCheck != nil {
		size += int64(48)
		hmap := reflect.ValueOf(cached.NoNeedToTypeCheck)
		numBuckets := int(math.Pow(2, float64((*(*uint8)(unsafe.Pointer(hmap.Pointer() + uintptr(9)))))))
		numOldBuckets := (*(*uint16)(unsafe.Pointer(hmap.Pointer() + uintptr(10))))
		size += hack.RuntimeAllocSize(int64(numOldBuckets * 208))
		if len(cached.NoNeedToTypeCheck) > 0 || numBuckets > 1 {
			size += hack.RuntimeAllocSize(int64(numBuckets * 208))
		}
	}
	return size
}
func (cached *DBDDL) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(32)
	}
	// field name string
	size += hack.RuntimeAllocSize(int64(len(cached.name)))
	return size
}
func (cached *DDL) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(64)
	}
	// field Keyspace *vitess.io/vitess/go/vt/vtgate/vindexes.Keyspace
	size += cached.Keyspace.CachedSize(true)
	// field SQL string
	size += hack.RuntimeAllocSize(int64(len(cached.SQL)))
	// field DDL vitess.io/vitess/go/vt/sqlparser.DDLStatement
	if cc, ok := cached.DDL.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field NormalDDL *vitess.io/vitess/go/vt/vtgate/engine.Send
	size += cached.NormalDDL.CachedSize(true)
	// field OnlineDDL *vitess.io/vitess/go/vt/vtgate/engine.OnlineDDL
	size += cached.OnlineDDL.CachedSize(true)
	return size
}
func (cached *DML) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(128)
	}
	// field Query string
	size += hack.RuntimeAllocSize(int64(len(cached.Query)))
	// field KsidVindex vitess.io/vitess/go/vt/vtgate/vindexes.Vindex
	if cc, ok := cached.KsidVindex.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field TableNames []string
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.TableNames)) * int64(16))
		for _, elem := range cached.TableNames {
			size += hack.RuntimeAllocSize(int64(len(elem)))
		}
	}
	// field Vindexes []*vitess.io/vitess/go/vt/vtgate/vindexes.ColumnVindex
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Vindexes)) * int64(8))
		for _, elem := range cached.Vindexes {
			size += elem.CachedSize(true)
		}
	}
	// field OwnedVindexQuery string
	size += hack.RuntimeAllocSize(int64(len(cached.OwnedVindexQuery)))
	// field RoutingParameters *vitess.io/vitess/go/vt/vtgate/engine.RoutingParameters
	size += cached.RoutingParameters.CachedSize(true)
	return size
}
func (cached *Delete) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(8)
	}
	// field DML *vitess.io/vitess/go/vt/vtgate/engine.DML
	size += cached.DML.CachedSize(true)
	return size
}
func (cached *Distinct) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(48)
	}
	// field Source vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Source.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field CheckCols []vitess.io/vitess/go/vt/vtgate/engine.CheckCol
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.CheckCols)) * int64(22))
		for _, elem := range cached.CheckCols {
			size += elem.CachedSize(false)
		}
	}
	return size
}
func (cached *ExecStmt) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(48)
	}
	// field Params []*vitess.io/vitess/go/vt/sqlparser.Variable
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Params)) * int64(8))
		for _, elem := range cached.Params {
			size += elem.CachedSize(true)
		}
	}
	// field Input vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Input.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *Filter) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(64)
	}
	// field Predicate vitess.io/vitess/go/vt/vtgate/evalengine.Expr
	if cc, ok := cached.Predicate.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field ASTPredicate vitess.io/vitess/go/vt/sqlparser.Expr
	if cc, ok := cached.ASTPredicate.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field Input vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Input.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *FkCascade) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(64)
	}
	// field Selection vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Selection.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field Children []*vitess.io/vitess/go/vt/vtgate/engine.FkChild
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Children)) * int64(8))
		for _, elem := range cached.Children {
			size += elem.CachedSize(true)
		}
	}
	// field Parent vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Parent.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *FkChild) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(64)
	}
	// field BVName string
	size += hack.RuntimeAllocSize(int64(len(cached.BVName)))
	// field Cols []int
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Cols)) * int64(8))
	}
	// field Exec vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Exec.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *FkParent) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(80)
	}
	// field Values []vitess.io/vitess/go/vt/sqlparser.Exprs
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Values)) * int64(24))
		for _, elem := range cached.Values {
			{
				size += hack.RuntimeAllocSize(int64(cap(elem)) * int64(16))
				for _, elem := range elem {
					if cc, ok := elem.(cachedObject); ok {
						size += cc.CachedSize(true)
					}
				}
			}
		}
	}
	// field Cols []vitess.io/vitess/go/vt/vtgate/engine.CheckCol
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Cols)) * int64(22))
		for _, elem := range cached.Cols {
			size += elem.CachedSize(false)
		}
	}
	// field BvName string
	size += hack.RuntimeAllocSize(int64(len(cached.BvName)))
	// field Exec vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Exec.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *FkVerify) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(48)
	}
	// field Verify []*vitess.io/vitess/go/vt/vtgate/engine.FkParent
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Verify)) * int64(8))
		for _, elem := range cached.Verify {
			size += elem.CachedSize(true)
		}
	}
	// field Exec vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Exec.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *Generate) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(48)
	}
	// field Keyspace *vitess.io/vitess/go/vt/vtgate/vindexes.Keyspace
	size += cached.Keyspace.CachedSize(true)
	// field Query string
	size += hack.RuntimeAllocSize(int64(len(cached.Query)))
	// field Values vitess.io/vitess/go/vt/vtgate/evalengine.Expr
	if cc, ok := cached.Values.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *GroupByParams) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(48)
	}
	// field Expr vitess.io/vitess/go/vt/sqlparser.Expr
	if cc, ok := cached.Expr.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *HashJoin) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(112)
	}
	// field Left vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Left.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field Right vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Right.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field Cols []int
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Cols)) * int64(8))
	}
	// field ASTPred vitess.io/vitess/go/vt/sqlparser.Expr
	if cc, ok := cached.ASTPred.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *Insert) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(240)
	}
	// field Keyspace *vitess.io/vitess/go/vt/vtgate/vindexes.Keyspace
	size += cached.Keyspace.CachedSize(true)
	// field Query string
	size += hack.RuntimeAllocSize(int64(len(cached.Query)))
	// field VindexValues [][][]vitess.io/vitess/go/vt/vtgate/evalengine.Expr
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.VindexValues)) * int64(24))
		for _, elem := range cached.VindexValues {
			{
				size += hack.RuntimeAllocSize(int64(cap(elem)) * int64(24))
				for _, elem := range elem {
					{
						size += hack.RuntimeAllocSize(int64(cap(elem)) * int64(16))
						for _, elem := range elem {
							if cc, ok := elem.(cachedObject); ok {
								size += cc.CachedSize(true)
							}
						}
					}
				}
			}
		}
	}
	// field ColVindexes []*vitess.io/vitess/go/vt/vtgate/vindexes.ColumnVindex
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.ColVindexes)) * int64(8))
		for _, elem := range cached.ColVindexes {
			size += elem.CachedSize(true)
		}
	}
	// field TableName string
	size += hack.RuntimeAllocSize(int64(len(cached.TableName)))
	// field Generate *vitess.io/vitess/go/vt/vtgate/engine.Generate
	size += cached.Generate.CachedSize(true)
	// field Prefix string
	size += hack.RuntimeAllocSize(int64(len(cached.Prefix)))
	// field Mid []string
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Mid)) * int64(16))
		for _, elem := range cached.Mid {
			size += hack.RuntimeAllocSize(int64(len(elem)))
		}
	}
	// field Suffix string
	size += hack.RuntimeAllocSize(int64(len(cached.Suffix)))
	// field VindexValueOffset [][]int
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.VindexValueOffset)) * int64(24))
		for _, elem := range cached.VindexValueOffset {
			{
				size += hack.RuntimeAllocSize(int64(cap(elem)) * int64(8))
			}
		}
	}
	// field Input vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Input.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}

//go:nocheckptr
func (cached *Join) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(80)
	}
	// field Left vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Left.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field Right vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Right.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field Cols []int
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Cols)) * int64(8))
	}
	// field Vars map[string]int
	if cached.Vars != nil {
		size += int64(48)
		hmap := reflect.ValueOf(cached.Vars)
		numBuckets := int(math.Pow(2, float64((*(*uint8)(unsafe.Pointer(hmap.Pointer() + uintptr(9)))))))
		numOldBuckets := (*(*uint16)(unsafe.Pointer(hmap.Pointer() + uintptr(10))))
		size += hack.RuntimeAllocSize(int64(numOldBuckets * 208))
		if len(cached.Vars) > 0 || numBuckets > 1 {
			size += hack.RuntimeAllocSize(int64(numBuckets * 208))
		}
		for k := range cached.Vars {
			size += hack.RuntimeAllocSize(int64(len(k)))
		}
	}
	return size
}
func (cached *Limit) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(48)
	}
	// field Count vitess.io/vitess/go/vt/vtgate/evalengine.Expr
	if cc, ok := cached.Count.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field Offset vitess.io/vitess/go/vt/vtgate/evalengine.Expr
	if cc, ok := cached.Offset.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field Input vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Input.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *Lock) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(64)
	}
	// field Keyspace *vitess.io/vitess/go/vt/vtgate/vindexes.Keyspace
	size += cached.Keyspace.CachedSize(true)
	// field TargetDestination vitess.io/vitess/go/vt/key.Destination
	if cc, ok := cached.TargetDestination.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field FieldQuery string
	size += hack.RuntimeAllocSize(int64(len(cached.FieldQuery)))
	// field LockFunctions []*vitess.io/vitess/go/vt/vtgate/engine.LockFunc
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.LockFunctions)) * int64(8))
		for _, elem := range cached.LockFunctions {
			size += elem.CachedSize(true)
		}
	}
	return size
}
func (cached *LockFunc) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(24)
	}
	// field Typ *vitess.io/vitess/go/vt/sqlparser.LockingFunc
	size += cached.Typ.CachedSize(true)
	// field Name vitess.io/vitess/go/vt/vtgate/evalengine.Expr
	if cc, ok := cached.Name.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *MStream) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(48)
	}
	// field Keyspace *vitess.io/vitess/go/vt/vtgate/vindexes.Keyspace
	size += cached.Keyspace.CachedSize(true)
	// field TargetDestination vitess.io/vitess/go/vt/key.Destination
	if cc, ok := cached.TargetDestination.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field TableName string
	size += hack.RuntimeAllocSize(int64(len(cached.TableName)))
	return size
}
func (cached *MemorySort) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(64)
	}
	// field UpperLimit vitess.io/vitess/go/vt/vtgate/evalengine.Expr
	if cc, ok := cached.UpperLimit.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field OrderBy []vitess.io/vitess/go/vt/vtgate/engine.OrderByParams
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.OrderBy)) * int64(38))
	}
	// field Input vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Input.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *MergeSort) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(64)
	}
	// field Primitives []vitess.io/vitess/go/vt/vtgate/engine.StreamExecutor
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Primitives)) * int64(16))
		for _, elem := range cached.Primitives {
			if cc, ok := elem.(cachedObject); ok {
				size += cc.CachedSize(true)
			}
		}
	}
	// field OrderBy []vitess.io/vitess/go/vt/vtgate/engine.OrderByParams
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.OrderBy)) * int64(38))
	}
	return size
}
func (cached *OnlineDDL) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(64)
	}
	// field Keyspace *vitess.io/vitess/go/vt/vtgate/vindexes.Keyspace
	size += cached.Keyspace.CachedSize(true)
	// field DDL vitess.io/vitess/go/vt/sqlparser.DDLStatement
	if cc, ok := cached.DDL.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field SQL string
	size += hack.RuntimeAllocSize(int64(len(cached.SQL)))
	// field DDLStrategySetting *vitess.io/vitess/go/vt/schema.DDLStrategySetting
	size += cached.DDLStrategySetting.CachedSize(true)
	// field TargetDestination vitess.io/vitess/go/vt/key.Destination
	if cc, ok := cached.TargetDestination.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *OrderedAggregate) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(80)
	}
	// field Aggregates []*vitess.io/vitess/go/vt/vtgate/engine.AggregateParams
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Aggregates)) * int64(8))
		for _, elem := range cached.Aggregates {
			size += elem.CachedSize(true)
		}
	}
	// field GroupByKeys []*vitess.io/vitess/go/vt/vtgate/engine.GroupByParams
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.GroupByKeys)) * int64(8))
		for _, elem := range cached.GroupByKeys {
			size += elem.CachedSize(true)
		}
	}
	// field Input vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Input.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *Plan) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(144)
	}
	// field Original string
	size += hack.RuntimeAllocSize(int64(len(cached.Original)))
	// field Instructions vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Instructions.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field BindVarNeeds *vitess.io/vitess/go/vt/sqlparser.BindVarNeeds
	size += cached.BindVarNeeds.CachedSize(true)
	// field Warnings []*vitess.io/vitess/go/vt/proto/query.QueryWarning
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Warnings)) * int64(8))
		for _, elem := range cached.Warnings {
			size += elem.CachedSize(true)
		}
	}
	// field TablesUsed []string
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.TablesUsed)) * int64(16))
		for _, elem := range cached.TablesUsed {
			size += hack.RuntimeAllocSize(int64(len(elem)))
		}
	}
	return size
}
func (cached *Projection) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(64)
	}
	// field Cols []string
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Cols)) * int64(16))
		for _, elem := range cached.Cols {
			size += hack.RuntimeAllocSize(int64(len(elem)))
		}
	}
	// field Exprs []vitess.io/vitess/go/vt/vtgate/evalengine.Expr
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Exprs)) * int64(16))
		for _, elem := range cached.Exprs {
			if cc, ok := elem.(cachedObject); ok {
				size += cc.CachedSize(true)
			}
		}
	}
	// field Input vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Input.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *RenameFields) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(64)
	}
	// field Cols []string
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Cols)) * int64(16))
		for _, elem := range cached.Cols {
			size += hack.RuntimeAllocSize(int64(len(elem)))
		}
	}
	// field Indices []int
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Indices)) * int64(8))
	}
	// field Input vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Input.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *ReplaceVariables) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(16)
	}
	// field Input vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Input.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *RevertMigration) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(48)
	}
	// field Keyspace *vitess.io/vitess/go/vt/vtgate/vindexes.Keyspace
	size += cached.Keyspace.CachedSize(true)
	// field Stmt *vitess.io/vitess/go/vt/sqlparser.RevertMigration
	size += cached.Stmt.CachedSize(true)
	// field Query string
	size += hack.RuntimeAllocSize(int64(len(cached.Query)))
	// field TargetDestination vitess.io/vitess/go/vt/key.Destination
	if cc, ok := cached.TargetDestination.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *Route) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(128)
	}
	// field Query string
	size += hack.RuntimeAllocSize(int64(len(cached.Query)))
	// field TableName string
	size += hack.RuntimeAllocSize(int64(len(cached.TableName)))
	// field FieldQuery string
	size += hack.RuntimeAllocSize(int64(len(cached.FieldQuery)))
	// field OrderBy []vitess.io/vitess/go/vt/vtgate/engine.OrderByParams
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.OrderBy)) * int64(38))
	}
	// field RoutingParameters *vitess.io/vitess/go/vt/vtgate/engine.RoutingParameters
	size += cached.RoutingParameters.CachedSize(true)
	return size
}

//go:nocheckptr
func (cached *RoutingParameters) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(112)
	}
	// field Keyspace *vitess.io/vitess/go/vt/vtgate/vindexes.Keyspace
	size += cached.Keyspace.CachedSize(true)
	// field SysTableTableSchema []vitess.io/vitess/go/vt/vtgate/evalengine.Expr
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.SysTableTableSchema)) * int64(16))
		for _, elem := range cached.SysTableTableSchema {
			if cc, ok := elem.(cachedObject); ok {
				size += cc.CachedSize(true)
			}
		}
	}
	// field SysTableTableName map[string]vitess.io/vitess/go/vt/vtgate/evalengine.Expr
	if cached.SysTableTableName != nil {
		size += int64(48)
		hmap := reflect.ValueOf(cached.SysTableTableName)
		numBuckets := int(math.Pow(2, float64((*(*uint8)(unsafe.Pointer(hmap.Pointer() + uintptr(9)))))))
		numOldBuckets := (*(*uint16)(unsafe.Pointer(hmap.Pointer() + uintptr(10))))
		size += hack.RuntimeAllocSize(int64(numOldBuckets * 272))
		if len(cached.SysTableTableName) > 0 || numBuckets > 1 {
			size += hack.RuntimeAllocSize(int64(numBuckets * 272))
		}
		for k, v := range cached.SysTableTableName {
			size += hack.RuntimeAllocSize(int64(len(k)))
			if cc, ok := v.(cachedObject); ok {
				size += cc.CachedSize(true)
			}
		}
	}
	// field TargetDestination vitess.io/vitess/go/vt/key.Destination
	if cc, ok := cached.TargetDestination.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field Vindex vitess.io/vitess/go/vt/vtgate/vindexes.Vindex
	if cc, ok := cached.Vindex.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field Values []vitess.io/vitess/go/vt/vtgate/evalengine.Expr
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Values)) * int64(16))
		for _, elem := range cached.Values {
			if cc, ok := elem.(cachedObject); ok {
				size += cc.CachedSize(true)
			}
		}
	}
	return size
}
func (cached *Rows) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(48)
	}
	// field rows [][]vitess.io/vitess/go/sqltypes.Value
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.rows)) * int64(24))
		for _, elem := range cached.rows {
			{
				size += hack.RuntimeAllocSize(int64(cap(elem)) * int64(56))
				for _, elem := range elem {
					size += elem.CachedSize(false)
				}
			}
		}
	}
	// field fields []*vitess.io/vitess/go/vt/proto/query.Field
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.fields)) * int64(8))
		for _, elem := range cached.fields {
			size += elem.CachedSize(true)
		}
	}
	return size
}
func (cached *SQLCalcFoundRows) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(32)
	}
	// field LimitPrimitive vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.LimitPrimitive.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field CountPrimitive vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.CountPrimitive.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *ScalarAggregate) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(48)
	}
	// field Aggregates []*vitess.io/vitess/go/vt/vtgate/engine.AggregateParams
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Aggregates)) * int64(8))
		for _, elem := range cached.Aggregates {
			size += elem.CachedSize(true)
		}
	}
	// field Input vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Input.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}

//go:nocheckptr
func (cached *SemiJoin) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(64)
	}
	// field Left vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Left.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field Right vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Right.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field Cols []int
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Cols)) * int64(8))
	}
	// field Vars map[string]int
	if cached.Vars != nil {
		size += int64(48)
		hmap := reflect.ValueOf(cached.Vars)
		numBuckets := int(math.Pow(2, float64((*(*uint8)(unsafe.Pointer(hmap.Pointer() + uintptr(9)))))))
		numOldBuckets := (*(*uint16)(unsafe.Pointer(hmap.Pointer() + uintptr(10))))
		size += hack.RuntimeAllocSize(int64(numOldBuckets * 208))
		if len(cached.Vars) > 0 || numBuckets > 1 {
			size += hack.RuntimeAllocSize(int64(numBuckets * 208))
		}
		for k := range cached.Vars {
			size += hack.RuntimeAllocSize(int64(len(k)))
		}
	}
	return size
}
func (cached *Send) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(48)
	}
	// field Keyspace *vitess.io/vitess/go/vt/vtgate/vindexes.Keyspace
	size += cached.Keyspace.CachedSize(true)
	// field TargetDestination vitess.io/vitess/go/vt/key.Destination
	if cc, ok := cached.TargetDestination.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field Query string
	size += hack.RuntimeAllocSize(int64(len(cached.Query)))
	return size
}
func (cached *SessionPrimitive) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(24)
	}
	// field name string
	size += hack.RuntimeAllocSize(int64(len(cached.name)))
	return size
}
func (cached *Set) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(48)
	}
	// field Ops []vitess.io/vitess/go/vt/vtgate/engine.SetOp
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Ops)) * int64(16))
		for _, elem := range cached.Ops {
			if cc, ok := elem.(cachedObject); ok {
				size += cc.CachedSize(true)
			}
		}
	}
	// field Input vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Input.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *ShowExec) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(16)
	}
	// field ShowFilter *vitess.io/vitess/go/vt/sqlparser.ShowFilter
	size += cached.ShowFilter.CachedSize(true)
	return size
}
func (cached *SimpleProjection) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(48)
	}
	// field Cols []int
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Cols)) * int64(8))
	}
	// field Input vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Input.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *SysVarCheckAndIgnore) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(64)
	}
	// field Name string
	size += hack.RuntimeAllocSize(int64(len(cached.Name)))
	// field Keyspace *vitess.io/vitess/go/vt/vtgate/vindexes.Keyspace
	size += cached.Keyspace.CachedSize(true)
	// field TargetDestination vitess.io/vitess/go/vt/key.Destination
	if cc, ok := cached.TargetDestination.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field Expr string
	size += hack.RuntimeAllocSize(int64(len(cached.Expr)))
	return size
}
func (cached *SysVarIgnore) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(32)
	}
	// field Name string
	size += hack.RuntimeAllocSize(int64(len(cached.Name)))
	// field Expr string
	size += hack.RuntimeAllocSize(int64(len(cached.Expr)))
	return size
}
func (cached *SysVarReservedConn) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(64)
	}
	// field Name string
	size += hack.RuntimeAllocSize(int64(len(cached.Name)))
	// field Keyspace *vitess.io/vitess/go/vt/vtgate/vindexes.Keyspace
	size += cached.Keyspace.CachedSize(true)
	// field TargetDestination vitess.io/vitess/go/vt/key.Destination
	if cc, ok := cached.TargetDestination.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field Expr string
	size += hack.RuntimeAllocSize(int64(len(cached.Expr)))
	return size
}
func (cached *SysVarSetAware) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(32)
	}
	// field Name string
	size += hack.RuntimeAllocSize(int64(len(cached.Name)))
	// field Expr vitess.io/vitess/go/vt/vtgate/evalengine.Expr
	if cc, ok := cached.Expr.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *ThrottleApp) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(16)
	}
	// field Keyspace *vitess.io/vitess/go/vt/vtgate/vindexes.Keyspace
	size += cached.Keyspace.CachedSize(true)
	// field ThrottledAppRule *vitess.io/vitess/go/vt/proto/topodata.ThrottledAppRule
	size += cached.ThrottledAppRule.CachedSize(true)
	return size
}
func (cached *UncorrelatedSubquery) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(80)
	}
	// field SubqueryResult string
	size += hack.RuntimeAllocSize(int64(len(cached.SubqueryResult)))
	// field HasValues string
	size += hack.RuntimeAllocSize(int64(len(cached.HasValues)))
	// field Subquery vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Subquery.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field Outer vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Outer.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}

//go:nocheckptr
func (cached *Update) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(16)
	}
	// field DML *vitess.io/vitess/go/vt/vtgate/engine.DML
	size += cached.DML.CachedSize(true)
	// field ChangedVindexValues map[string]*vitess.io/vitess/go/vt/vtgate/engine.VindexValues
	if cached.ChangedVindexValues != nil {
		size += int64(48)
		hmap := reflect.ValueOf(cached.ChangedVindexValues)
		numBuckets := int(math.Pow(2, float64((*(*uint8)(unsafe.Pointer(hmap.Pointer() + uintptr(9)))))))
		numOldBuckets := (*(*uint16)(unsafe.Pointer(hmap.Pointer() + uintptr(10))))
		size += hack.RuntimeAllocSize(int64(numOldBuckets * 208))
		if len(cached.ChangedVindexValues) > 0 || numBuckets > 1 {
			size += hack.RuntimeAllocSize(int64(numBuckets * 208))
		}
		for k, v := range cached.ChangedVindexValues {
			size += hack.RuntimeAllocSize(int64(len(k)))
			size += v.CachedSize(true)
		}
	}
	return size
}
func (cached *UpdateTarget) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(16)
	}
	// field Target string
	size += hack.RuntimeAllocSize(int64(len(cached.Target)))
	return size
}
func (cached *UserDefinedVariable) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(32)
	}
	// field Name string
	size += hack.RuntimeAllocSize(int64(len(cached.Name)))
	// field Expr vitess.io/vitess/go/vt/vtgate/evalengine.Expr
	if cc, ok := cached.Expr.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *VExplain) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(24)
	}
	// field Input vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Input.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *VStream) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(64)
	}
	// field Keyspace *vitess.io/vitess/go/vt/vtgate/vindexes.Keyspace
	size += cached.Keyspace.CachedSize(true)
	// field TargetDestination vitess.io/vitess/go/vt/key.Destination
	if cc, ok := cached.TargetDestination.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field TableName string
	size += hack.RuntimeAllocSize(int64(len(cached.TableName)))
	// field Position string
	size += hack.RuntimeAllocSize(int64(len(cached.Position)))
	return size
}
func (cached *VindexFunc) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(96)
	}
	// field Fields []*vitess.io/vitess/go/vt/proto/query.Field
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Fields)) * int64(8))
		for _, elem := range cached.Fields {
			size += elem.CachedSize(true)
		}
	}
	// field Cols []int
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Cols)) * int64(8))
	}
	// field Vindex vitess.io/vitess/go/vt/vtgate/vindexes.SingleColumn
	if cc, ok := cached.Vindex.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field Value vitess.io/vitess/go/vt/vtgate/evalengine.Expr
	if cc, ok := cached.Value.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *VindexLookup) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(112)
	}
	// field Vindex vitess.io/vitess/go/vt/vtgate/vindexes.LookupPlanable
	if cc, ok := cached.Vindex.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field Keyspace *vitess.io/vitess/go/vt/vtgate/vindexes.Keyspace
	size += cached.Keyspace.CachedSize(true)
	// field Arguments []string
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Arguments)) * int64(16))
		for _, elem := range cached.Arguments {
			size += hack.RuntimeAllocSize(int64(len(elem)))
		}
	}
	// field Values []vitess.io/vitess/go/vt/vtgate/evalengine.Expr
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Values)) * int64(16))
		for _, elem := range cached.Values {
			if cc, ok := elem.(cachedObject); ok {
				size += cc.CachedSize(true)
			}
		}
	}
	// field Lookup vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.Lookup.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field SendTo *vitess.io/vitess/go/vt/vtgate/engine.Route
	size += cached.SendTo.CachedSize(true)
	return size
}

//go:nocheckptr
func (cached *VindexValues) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(16)
	}
	// field PvMap map[string]vitess.io/vitess/go/vt/vtgate/evalengine.Expr
	if cached.PvMap != nil {
		size += int64(48)
		hmap := reflect.ValueOf(cached.PvMap)
		numBuckets := int(math.Pow(2, float64((*(*uint8)(unsafe.Pointer(hmap.Pointer() + uintptr(9)))))))
		numOldBuckets := (*(*uint16)(unsafe.Pointer(hmap.Pointer() + uintptr(10))))
		size += hack.RuntimeAllocSize(int64(numOldBuckets * 272))
		if len(cached.PvMap) > 0 || numBuckets > 1 {
			size += hack.RuntimeAllocSize(int64(numBuckets * 272))
		}
		for k, v := range cached.PvMap {
			size += hack.RuntimeAllocSize(int64(len(k)))
			if cc, ok := v.(cachedObject); ok {
				size += cc.CachedSize(true)
			}
		}
	}
	return size
}
func (cached *VitessMetadata) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(32)
	}
	// field Name string
	size += hack.RuntimeAllocSize(int64(len(cached.Name)))
	// field Value string
	size += hack.RuntimeAllocSize(int64(len(cached.Value)))
	return size
}

//go:nocheckptr
func (cached *shardRoute) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(48)
	}
	// field query string
	size += hack.RuntimeAllocSize(int64(len(cached.query)))
	// field rs *vitess.io/vitess/go/vt/srvtopo.ResolvedShard
	size += cached.rs.CachedSize(true)
	// field bv map[string]*vitess.io/vitess/go/vt/proto/query.BindVariable
	if cached.bv != nil {
		size += int64(48)
		hmap := reflect.ValueOf(cached.bv)
		numBuckets := int(math.Pow(2, float64((*(*uint8)(unsafe.Pointer(hmap.Pointer() + uintptr(9)))))))
		numOldBuckets := (*(*uint16)(unsafe.Pointer(hmap.Pointer() + uintptr(10))))
		size += hack.RuntimeAllocSize(int64(numOldBuckets * 208))
		if len(cached.bv) > 0 || numBuckets > 1 {
			size += hack.RuntimeAllocSize(int64(numBuckets * 208))
		}
		for k, v := range cached.bv {
			size += hack.RuntimeAllocSize(int64(len(k)))
			size += v.CachedSize(true)
		}
	}
	// field primitive vitess.io/vitess/go/vt/vtgate/engine.Primitive
	if cc, ok := cached.primitive.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
