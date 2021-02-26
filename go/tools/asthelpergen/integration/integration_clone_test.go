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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClone(t *testing.T) {
	leaf1 := &Leaf{1}
	clone := leaf1.Clone()
	assert.Equal(t, leaf1, clone)
}

func TestClone2(t *testing.T) {
	container := &RefContainer{
		ASTType:               &RefContainer{},
		NotASTType:            0,
		ASTImplementationType: &Leaf{2},
	}
	clone := container.Clone()
	assert.Equal(t, container, clone)
}
