/*
Copyright 2017 Google Inc.

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

package worker

import (
  "golang.org/x/net/context"
  "vitess.io/vitess/go/vt/concurrency"
  "vitess.io/vitess/go/vt/logutil"
  "vitess.io/vitess/go/vt/proto/topodata"
  "vitess.io/vitess/go/vt/topo"
  "vitess.io/vitess/go/vt/vterrors"
  "vitess.io/vitess/go/vt/vttablet/tmclient"
  "vitess.io/vitess/go/vt/wrangler"
)

type ConsistentSnapshotDiffer struct {
  logger  logutil.Logger
  cleaner *wrangler.Cleaner
}

func NewConsistentSnapshotDiffer(logger logutil.Logger) Differ {
  x := ConsistentSnapshotDiffer{
    logger:  logger,
    cleaner: &wrangler.Cleaner{},
  }

  return &x
}

func (*ConsistentSnapshotDiffer) StabilizeSourceAndDestination(ctx context.Context,
  toposerver *topo.Server,
  tabletManagerClient tmclient.TabletManagerClient,
  shardInfo *topo.ShardInfo,
  sourceAlias *topodata.TabletAlias,
  destinationAlias *topodata.TabletAlias) error {
  panic("implement me")
}

func (*ConsistentSnapshotDiffer) Diff(ctx context.Context,
  wr *wrangler.Wrangler,
  sourceAlias *topodata.TabletAlias,
  destinationAlias *topodata.TabletAlias,
  shardInfo *topo.ShardInfo,
  markAsWillFail func(rec concurrency.ErrorRecorder, err error),
  parallelDiffsCount int) error {
  schemaInfo, err := SchemaDiff(ctx, wr, sourceAlias, destinationAlias, shardInfo, markAsWillFail)
  if err != nil {
    return vterrors.Wrapf(err, "schema diff failed")
  }
  panic(schemaInfo)
}
