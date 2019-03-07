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

  // written during StabilizeSourceAndDestination
  lastPos         string // contains the GTID position for the source
  srcTransactions []int64
  dstTransactions []int64
}

func NewConsistentSnapshotDiffer(logger logutil.Logger) Differ {
  x := ConsistentSnapshotDiffer{
    logger:  logger,
    cleaner: &wrangler.Cleaner{},
  }

  return &x
}

// StabilizeSourceAndDestination phase:
// 1 - ask the master of the destination shard to pause filtered replication
//     (add a cleanup task to restart filtered replication on master)
// 2 - create consistent snapshot read-only transactions on source tables
//     and note the gtid we arrived at
// 3 - ask the master of the destination shard to resume filtered replication
//     up to the new list of positions, and return its binlog position.
// 4 - wait until the destination tablet is equal or passed that master
//     binlog position
// 5 - create consistent snapshot read-only transactions on destination tables
// At this point, all source and destination transactions should see the same data
func (csd *ConsistentSnapshotDiffer) StabilizeSourceAndDestination(ctx context.Context,
  toposerver *topo.Server,
  tabletManagerClient tmclient.TabletManagerClient,
  shardInfo *topo.ShardInfo,
  sourceAlias *topodata.TabletAlias,
  destinationAlias *topodata.TabletAlias) error {

  shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
  ti, err := toposerver.GetTablet(shortCtx, sourceAlias)
  cancel()
  if err != nil {
    return vterrors.Wrapf(err, "trying to get tablet info for %v failed", sourceAlias)
  }

  sourceTransactions, gtid, err := CreateConsistentTransactions(ctx, ti, csd.logger, csd.cleaner, 8)
  if err != nil {
    return err
  }
  csd.lastPos = gtid
  csd.srcTransactions = sourceTransactions
  return nil
}

func (csd *ConsistentSnapshotDiffer) Diff(ctx context.Context,
  wr *wrangler.Wrangler,
  sourceAlias *topodata.TabletAlias,
  destinationAlias *topodata.TabletAlias,
  shardInfo *topo.ShardInfo,
  markAsWillFail func(rec concurrency.ErrorRecorder, err error),
  parallelDiffsCount int) error {

  _, err := SchemaDiff(ctx, wr, sourceAlias, destinationAlias, shardInfo, markAsWillFail)
  if err != nil {
    return vterrors.Wrapf(err, "schema diff failed")
  }

  panic("implement me")
}

