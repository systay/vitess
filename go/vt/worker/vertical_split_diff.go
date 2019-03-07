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
	"fmt"
	"html/template"

	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/wrangler"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// VerticalSplitDiffWorker executes a diff between a destination shard and its
// source shards in a shard split case.
type VerticalSplitDiffWorker struct {
	StatusWorker

	wr                      *wrangler.Wrangler
	cell                    string
	keyspace                string
	shard                   string
	minHealthyRdonlyTablets int
	parallelDiffsCount      int
	cleaner                 *wrangler.Cleaner
	useConsistentSnapshot   bool

	// populated during WorkerStateInit, read-only after that
	keyspaceInfo *topo.KeyspaceInfo
	shardInfo    *topo.ShardInfo

	// populated during WorkerStateFindTargets, read-only after that
	sourceAlias           *topodatapb.TabletAlias
	destinationAlias      *topodatapb.TabletAlias
	destinationTabletType topodatapb.TabletType

	// populated during WorkerStateDiff
	sourceSchemaDefinition      *tabletmanagerdatapb.SchemaDefinition
	destinationSchemaDefinition *tabletmanagerdatapb.SchemaDefinition
}

// NewVerticalSplitDiffWorker returns a new VerticalSplitDiffWorker object.
func NewVerticalSplitDiffWorker(wr *wrangler.Wrangler, cell, keyspace, shard string, minHealthyRdonlyTablets, parallelDiffsCount int, destinationTabletType topodatapb.TabletType) Worker {
	return &VerticalSplitDiffWorker{
		StatusWorker:            NewStatusWorker(),
		wr:                      wr,
		cell:                    cell,
		keyspace:                keyspace,
		shard:                   shard,
		minHealthyRdonlyTablets: minHealthyRdonlyTablets,
		destinationTabletType:   destinationTabletType,
		parallelDiffsCount:      parallelDiffsCount,
		cleaner:                 &wrangler.Cleaner{},
	}
}

// StatusAsHTML is part of the Worker interface.
func (vsdw *VerticalSplitDiffWorker) StatusAsHTML() template.HTML {
	state := vsdw.State()

	result := "<b>Working on:</b> " + vsdw.keyspace + "/" + vsdw.shard + "</br>\n"
	result += "<b>State:</b> " + state.String() + "</br>\n"
	switch state {
	case WorkerStateDiff:
		result += "<b>Running</b>:</br>\n"
	case WorkerStateDiffWillFail:
		result += "<b>Running - have already found differences...</b></br>\n"
	case WorkerStateDone:
		result += "<b>Success</b>:</br>\n"
	}

	return template.HTML(result)
}

// StatusAsText is part of the Worker interface.
func (vsdw *VerticalSplitDiffWorker) StatusAsText() string {
	state := vsdw.State()

	result := "Working on: " + vsdw.keyspace + "/" + vsdw.shard + "\n"
	result += "State: " + state.String() + "\n"
	switch state {
	case WorkerStateDiff:
		result += "Running...\n"
	case WorkerStateDiffWillFail:
		result += "Running - have already found differences...\n"
	case WorkerStateDone:
		result += "Success.\n"
	}
	return result
}

// Run is mostly a wrapper to run the cleanup at the end.
func (vsdw *VerticalSplitDiffWorker) Run(ctx context.Context) error {
	resetVars()
	err := vsdw.run(ctx)

	vsdw.SetState(WorkerStateCleanUp)
	cerr := vsdw.cleaner.CleanUp(vsdw.wr)
	if cerr != nil {
		if err != nil {
			vsdw.wr.Logger().Errorf2(cerr, "CleanUp failed in addition to job error")
		} else {
			err = cerr
		}
	}
	if err != nil {
		vsdw.SetState(WorkerStateError)
		return err
	}
	vsdw.SetState(WorkerStateDone)
	return nil
}

func (vsdw *VerticalSplitDiffWorker) run(ctx context.Context) error {
	// first state: read what we need to do
	if err := vsdw.init(ctx); err != nil {
		return vterrors.Wrap(err, "init() failed")
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// second state: find targets
	if err := vsdw.findTargets(ctx); err != nil {
		return vterrors.Wrap(err, "findTargets() failed")
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// third phase: synchronize replication
	differ := NewPausedReplicationDiffer(vsdw.wr.Logger())
	vsdw.SetState(WorkerStateSyncReplication)
	if err := differ.StabilizeSourceAndDestination(ctx, vsdw.wr.TopoServer(), vsdw.wr.TabletManagerClient(), vsdw.shardInfo, vsdw.sourceAlias, vsdw.destinationAlias); err != nil {
		return vterrors.Wrap(err, "synchronizeReplication() failed")
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// fourth phase: diff
	vsdw.SetState(WorkerStateDiff)
	if err := differ.Diff(ctx, vsdw.wr, vsdw.sourceAlias, vsdw.destinationAlias, vsdw.shardInfo, vsdw.markAsWillFail, vsdw.parallelDiffsCount); err != nil {
		return vterrors.Wrap(err, "diff() failed")
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	return nil
}

// init phase:
// - read the shard info, make sure it has sources
func (vsdw *VerticalSplitDiffWorker) init(ctx context.Context) error {
	vsdw.SetState(WorkerStateInit)

	var err error

	// read the keyspace and validate it
	vsdw.keyspaceInfo, err = vsdw.wr.TopoServer().GetKeyspace(ctx, vsdw.keyspace)
	if err != nil {
		return vterrors.Wrapf(err, "cannot read keyspace %v", vsdw.keyspace)
	}
	if len(vsdw.keyspaceInfo.ServedFroms) == 0 {
		return fmt.Errorf("keyspace %v has no KeyspaceServedFrom", vsdw.keyspace)
	}

	// read the shardinfo and validate it
	vsdw.shardInfo, err = vsdw.wr.TopoServer().GetShard(ctx, vsdw.keyspace, vsdw.shard)
	if err != nil {
		return vterrors.Wrapf(err, "cannot read shard %v/%v", vsdw.keyspace, vsdw.shard)
	}
	if len(vsdw.shardInfo.SourceShards) != 1 {
		return fmt.Errorf("shard %v/%v has bad number of source shards", vsdw.keyspace, vsdw.shard)
	}
	if len(vsdw.shardInfo.SourceShards[0].Tables) == 0 {
		return fmt.Errorf("shard %v/%v has no tables in source shard[0]", vsdw.keyspace, vsdw.shard)
	}
	if !vsdw.shardInfo.HasMaster() {
		return fmt.Errorf("shard %v/%v has no master", vsdw.keyspace, vsdw.shard)
	}

	return nil
}

// findTargets phase:
// - find one destinationTabletType in destination shard
// - find one rdonly per source shard
// - mark them all as 'worker' pointing back to us
func (vsdw *VerticalSplitDiffWorker) findTargets(ctx context.Context) error {
	vsdw.SetState(WorkerStateFindTargets)

	// find an appropriate tablet in destination shard
	var err error
	vsdw.destinationAlias, err = FindWorkerTablet(
		ctx,
		vsdw.wr,
		vsdw.cleaner,
		nil, /* tsc */
		vsdw.cell,
		vsdw.keyspace,
		vsdw.shard,
		1, /* minHealthyTablets */
		vsdw.destinationTabletType,
	)
	if err != nil {
		return vterrors.Wrapf(err, "FindWorkerTablet() failed for %v/%v/%v", vsdw.cell, vsdw.keyspace, vsdw.shard)
	}

	// find an appropriate tablet in the source shard
	vsdw.sourceAlias, err = FindWorkerTablet(ctx, vsdw.wr, vsdw.cleaner, nil /* tsc */, vsdw.cell, vsdw.shardInfo.SourceShards[0].Keyspace, vsdw.shardInfo.SourceShards[0].Shard, vsdw.minHealthyRdonlyTablets, topodatapb.TabletType_RDONLY)
	if err != nil {
		return vterrors.Wrapf(err, "FindWorkerTablet() failed for %v/%v/%v", vsdw.cell, vsdw.shardInfo.SourceShards[0].Keyspace, vsdw.shardInfo.SourceShards[0].Shard)
	}

	return nil
}

// markAsWillFail records the error and changes the state of the worker to reflect this
func (vsdw *VerticalSplitDiffWorker) markAsWillFail(er concurrency.ErrorRecorder, err error) {
	er.RecordError(err)
	vsdw.SetState(WorkerStateDiffWillFail)
}

type Differ interface {
	StabilizeSourceAndDestination(ctx context.Context,
		toposerver *topo.Server,
		tabletManagerClient tmclient.TabletManagerClient,
		shardInfo *topo.ShardInfo,
		sourceAlias *topodatapb.TabletAlias,
		destinationAlias *topodatapb.TabletAlias) error

	Diff(ctx context.Context,
		wr *wrangler.Wrangler,
		sourceAlias *topodatapb.TabletAlias,
		destinationAlias *topodatapb.TabletAlias,
		shardInfo *topo.ShardInfo,
		markAsWillFail func(rec concurrency.ErrorRecorder, err error),
		parallelDiffsCount int) error
}

