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

package main

import (
	"log"
	"os"
	"strings"
	"text/template"

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/vt/vterrors"
)

const (
	tmpl = `## Errors

| ID | Description | Error | MySQL Error Code | SQL State |
| --- | --- | --- | --- | --- |
{{- range $err := . }}
{{- $data := (call $err) }}
| {{ $data.ID }} | {{ $data.Description }} | {{ FormatError $data.Err }} | {{ ConvertStateToMySQLErrorCode $data.State }} | {{ ConvertStateToMySQLState $data.State }} |
{{- end }}
`
)

func main() {
	t := template.New("template")
	t.Funcs(map[string]any{
		"ConvertStateToMySQLErrorCode": mysql.ConvertStateToMySQLErrorCode,
		"ConvertStateToMySQLState":     mysql.ConvertStateToMySQLState,
		"FormatError": func(err error) string {
			s := err.Error()
			return strings.TrimSpace(strings.Join(strings.Split(s, ":")[1:], ":"))
		},
	})
	parse, err := t.Parse(tmpl)
	t = template.Must(parse, err)

	err = t.ExecuteTemplate(os.Stdout, "template", vterrors.Errors)
	if err != nil {
		log.Fatal(err)
	}
}
