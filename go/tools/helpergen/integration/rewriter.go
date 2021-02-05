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

package integration

func replaceAfield1(newNode, parent SQLNode) {
	parent.(A).field1 = newNode.(uint64)
}
func replaceAfield2(newNode, parent SQLNode) {
	parent.(A).field2 = newNode.(uint64)
}
func replaceBimplfield1(newNode, parent SQLNode) {
	parent.(Bimpl).field1 = newNode.(uint64)
}
func replaceCfield1(newNode, parent SQLNode) {
	parent.(C).field1 = newNode.(B)
}
func replaceDfield1(newNode, parent SQLNode) {
	parent.(D).field1 = newNode.(*Bimpl)
}
func replaceMap1field1(newNode, parent SQLNode) {
	parent.(Map1).field1 = newNode.(map[uint8]uint8)
}
func replaceMap2field1(newNode, parent SQLNode) {
	parent.(Map2).field1 = newNode.(map[uint64]A)
}
func replaceMap3field1(newNode, parent SQLNode) {
	parent.(Map3).field1 = newNode.(map[uint64]B)
}
func replacePaddedfield1(newNode, parent SQLNode) {
	parent.(Padded).field1 = newNode.(uint64)
}
func replacePaddedfield2(newNode, parent SQLNode) {
	parent.(Padded).field2 = newNode.(uint8)
}
func replacePaddedfield3(newNode, parent SQLNode) {
	parent.(Padded).field3 = newNode.(uint64)
}
func replaceSlice1field1(newNode, parent SQLNode) {
	parent.(Slice1).field1 = newNode.([]A)
}
func replaceSlice2field1(newNode, parent SQLNode) {
	parent.(Slice2).field1 = newNode.([]B)
}
func replaceSlice3field1(newNode, parent SQLNode) {
	parent.(Slice3).field1 = newNode.([]Bimpl)
}
func replaceString1field1(newNode, parent SQLNode) {
	parent.(String1).field1 = newNode.(string)
}
func replaceString1field2(newNode, parent SQLNode) {
	parent.(String1).field2 = newNode.(uint64)
}
