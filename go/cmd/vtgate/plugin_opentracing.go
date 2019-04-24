/*
Copyright 2018 The Vitess Authors.

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

func init() {
  // We are starting the tracing system through servenv.OnRun(), and
  // vtgate.Init() runs before the servenv has been started.
  // These two facts makes it so we are not tracing anything happening in vtgate.Init(). This will lead to spans being
  // created on other services that have no root span when we expect one. If you want to look into what is happening in
  // vtgate.Init(), move the tracer starting to the main method instead.
  //servenv.OnRun(func() {
  //  closer := trace.StartTracing("vtgate")
  //  servenv.OnClose(trace.LogErrorsWhenClosing(closer))
  //})
}
