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

package main

import (
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFullSizeGeneration(t *testing.T) {
	result, err := GenerateHelpers([]string{"./integration/..."}, []string{"vitess.io/vitess/go/tools/helpergen/integration.*"}, newCachedSize)
	require.NoError(t, err)

	verifyErrors := VerifyFilesOnDisk(result)
	require.Empty(t, verifyErrors)

	for _, file := range result {
		contents := fmt.Sprintf("%#v", file)
		require.Contains(t, contents, "http://www.apache.org/licenses/LICENSE-2.0")
		require.Contains(t, contents, "type cachedObject interface")
		require.Contains(t, contents, "//go:nocheckptr")
	}
}

func TestFullRewriterGeneration(t *testing.T) {
	result, err := GenerateHelpers([]string{"./integration/..."}, []string{"vitess.io/vitess/go/tools/helpergen/integration.*"}, newRewriterGen)
	require.NoError(t, err)
	for fullPath, file := range result {
		if err := file.Save(fullPath); err != nil {
			log.Fatalf("filed to save file to '%s': %v", fullPath, err)
		}
		log.Printf("saved '%s'", fullPath)
	}

	verifyErrors := VerifyFilesOnDisk(result)
	require.Empty(t, verifyErrors)

	for _, file := range result {
		contents := fmt.Sprintf("%#v", file)
		require.Contains(t, contents, "http://www.apache.org/licenses/LICENSE-2.0")
	}
}
