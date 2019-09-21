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
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var licenseFilePtr = flag.String("license-file", "", "what should the correct license header look like")
var dirPtr = flag.String("dir", "", "where to recursively look for source files")
var testOnly = flag.Bool("test-only", true, "when set to true, will not change anything and just return a non-zero result if not all files are correct")
var exclude = flag.String("exclude", "", "comma separated list of patterns of file to exclude from checking")

var licenseLines []string
var license string

var badFiles []string

func main() {
	flag.Parse()

	buf, err := ioutil.ReadFile(*licenseFilePtr)
	panicOnError(err)
	setLicense(string(buf))
	var wg sync.WaitGroup

	badFileHandle := pickBadFileHandler()
	root, err := filepath.Abs(*dirPtr)
	panicOnError(err)
	err = filepath.Walk(root, func(path string, file os.FileInfo, err error) error {
		fileName := file.Name()
		isGoFile, err := filepath.Match("*.go", fileName)
		panicOnError(err)
		if !file.IsDir() && isGoFile && !shouldExclude(fileName) {
			wg.Add(1)
			go func() {
				checkFile(path, badFileHandle)
				wg.Done()
			}()
		}
		return nil
	})
	panicOnError(err)

	wg.Wait()

	if len(badFiles) > 0 {
		if *testOnly {
			fmt.Println("❌ These files did not have the correct license header:\n   " + strings.Join(badFiles, "\n   "))
			os.Exit(1)
		}
		fmt.Println(fmt.Sprintf("Replaced %d license headers", len(badFiles)))
	}

	os.Exit(0)
}

func shouldExclude(file string) bool {
	paths := strings.Split(*exclude, ",")
	for _, pattern := range paths {
		excluded, err := filepath.Match(pattern, file)
		panicOnError(err)
		if excluded {
			return true
		}
	}
	return false
}

func setLicense(s string) {
	license = strings.Trim(s, "\n")
	licenseLines = strings.Split(license, "\n")
}

func pickBadFileHandler() func(string, []string) {
	if *testOnly {
		return rememberBadFiles
	}
	return replaceBadFile
}

func panicOnError(e error) {
	if e != nil {
		panic(e)
	}
}

func replaceBadFile(fullPath string, fileLines []string) {
	newContent := replaceLicense(fileLines, license)
	panicOnError(ioutil.WriteFile(fullPath, []byte(newContent), os.ModePerm))
	badFiles = append(badFiles, fullPath)
}

func rememberBadFiles(fullPath string, _ []string) {
	dir, e := os.Getwd()
	panicOnError(e)
	s, e := filepath.Rel(dir, fullPath)
	panicOnError(e)
	badFiles = append(badFiles, s)
}

func checkFile(fullPath string, badFileHandle func(fullPath string, lines []string)) {
	buf, err := ioutil.ReadFile(fullPath)
	panicOnError(err)
	fileContent := string(buf)
	fileLines := strings.Split(fileContent, "\n")

	if !doesFileHaveCorrectHeader(fileLines) {
		badFileHandle(fullPath, fileLines)
	}
}

func doesFileHaveCorrectHeader(fileLines []string) bool {
	if len(licenseLines) == 0 {
		panic("no license configured")
	}

	_, idx := spoolPastBuildDirectives(fileLines)

	for i, licenseLine := range licenseLines {
		fileLine := fileLines[i+idx]
		if licenseLine != fileLine {
			return false
		}
	}
	return true
}

func replaceLicense(lines []string, licenseHeader string) string {
	// If the source file starts with empty lines, give up. We can probably do better,
	// but it was not necessary given this code base
	if len(lines) > 0 && lines[0] == "" {
		panic(strings.Join(lines, "\n"))
	}

	prefix, idx := spoolPastBuildDirectives(lines)

	if len(lines) > idx && strings.HasPrefix(lines[idx], "/*") {
		// continue until we find the end of the block comment
		for idx < len(lines) && lines[idx] != "*/" {
			idx++
		}
		idx++ // this last one so we are pointing to the first line after the comment, not the end of the comment it self
		if idx >= len(lines) {
			panic("found no end of comment")
		}
	}

	return prefix + licenseHeader + "\n" + strings.Join(lines[idx:], "\n")
}

// go build allows for build directives to be added to the very top of a source file. The rules are that
// nothing can come before it, except other line comments. So we spool past all line comments, and insert
// our license header after. One alternative could be to use a line commented version of the license.
func spoolPastBuildDirectives(lines []string) (prefix string, idx int) {
	if len(lines) > 0 && strings.HasPrefix(lines[0], "//") {
		// continue until we find the eof or a non-comment line
		for idx < len(lines) && strings.HasPrefix(lines[idx], "//") {
			idx++
		}
		// remember the line comments before - we'll add our license under
		prefix = strings.Join(lines[0:idx], "\n") + "\n"
	}

	if strings.Contains(prefix, "+build") && lines[idx] == "" {
		idx++
		prefix += "\n"
	} else {
		// this wasn't a build directive comment block, so we can insert our license block before
		idx = 0
		prefix = ""
	}
	return prefix, idx
}
