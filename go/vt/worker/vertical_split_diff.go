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
	"sync"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
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

type PausedReplicationDiffer struct {
	logger  logutil.Logger
	cleaner *wrangler.Cleaner
}

func NewPausedReplicationDiffer(logger logutil.Logger) Differ {
	x := PausedReplicationDiffer{
		logger:  logger,
		cleaner: &wrangler.Cleaner{},
	}

	return &x
}

// StabilizeSourceAndDestination phase:
// 1 - ask the master of the destination shard to pause filtered replication,
//   and return the source binlog positions
//   (add a cleanup task to restart filtered replication on master)
// 2 - stop the source tablet at a binlog position higher than the
//   destination master. Get that new position.
//   (add a cleanup task to restart binlog replication on it, and change
//    the existing ChangeSlaveType cleanup action to 'spare' type)
// 3 - ask the master of the destination shard to resume filtered replication
//   up to the new list of positions, and return its binlog position.
// 4 - wait until the destination tablet is equal or passed that master
//   binlog position, and stop its replication.
//   (add a cleanup task to restart binlog replication on it, and change
//    the existing ChangeSlaveType cleanup action to 'spare' type)
// 5 - restart filtered replication on destination master.
//   (remove the cleanup task that does the same)
// At this point, all source and destination tablets are stopped at the same point.
func (prd *PausedReplicationDiffer) StabilizeSourceAndDestination(ctx context.Context,
	toposerver *topo.Server,
	tabletManagerClient tmclient.TabletManagerClient,
	shardInfo *topo.ShardInfo,
	sourceAlias *topodatapb.TabletAlias,
	destinationAlias *topodatapb.TabletAlias) error {

	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	defer cancel()
	masterInfo, err := toposerver.GetTablet(shortCtx, shardInfo.MasterAlias)
	if err != nil {
		return vterrors.Wrapf(err, "synchronizeReplication: cannot get Tablet record for master %v", topoproto.TabletAliasString(shardInfo.MasterAlias))
	}

	ss := shardInfo.SourceShards[0]

	// 1 - stop the master binlog replication, get its current position
	prd.logger.Infof("Stopping master binlog replication on %v", topoproto.TabletAliasString(shardInfo.MasterAlias))
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	defer cancel()
	_, err = tabletManagerClient.VReplicationExec(shortCtx, masterInfo.Tablet, binlogplayer.StopVReplication(ss.Uid, "for split diff"))
	if err != nil {
		return vterrors.Wrapf(err, "Stop VReplication on master %v failed", topoproto.TabletAliasString(shardInfo.MasterAlias))
	}
	wrangler.RecordVReplicationAction(prd.cleaner, masterInfo.Tablet, binlogplayer.StartVReplication(ss.Uid))
	p3qr, err := tabletManagerClient.VReplicationExec(shortCtx, masterInfo.Tablet, binlogplayer.ReadVReplicationPos(ss.Uid))
	if err != nil {
		return vterrors.Wrapf(err, "VReplicationExec(stop) for %v failed", shardInfo.MasterAlias)
	}
	qr := sqltypes.Proto3ToResult(p3qr)
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return fmt.Errorf("unexpected result while reading position: %v", qr)
	}
	vreplicationPos := qr.Rows[0][0].ToString()

	// stop replication
	prd.logger.Infof("Stopping slave %v at a minimum of %v", topoproto.TabletAliasString(sourceAlias), vreplicationPos)
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	defer cancel()
	sourceTablet, err := toposerver.GetTablet(shortCtx, sourceAlias)
	if err != nil {
		return err
	}
	mysqlPos, err := tabletManagerClient.StopSlaveMinimum(shortCtx, sourceTablet.Tablet, vreplicationPos, *remoteActionsTimeout)
	if err != nil {
		return vterrors.Wrapf(err, "cannot stop slave %v at right binlog position %v", topoproto.TabletAliasString(sourceAlias), vreplicationPos)
	}

	// change the cleaner actions from ChangeSlaveType(rdonly)
	// to StartSlave() + ChangeSlaveType(spare)
	wrangler.RecordStartSlaveAction(prd.cleaner, sourceTablet.Tablet)

	// 3 - ask the master of the destination shard to resume filtered
	//     replication up to the new list of positions
	prd.logger.Infof("Restarting master %v until it catches up to %v", topoproto.TabletAliasString(shardInfo.MasterAlias), mysqlPos)
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	defer cancel()
	_, err = tabletManagerClient.VReplicationExec(shortCtx, masterInfo.Tablet, binlogplayer.StartVReplicationUntil(ss.Uid, mysqlPos))
	if err != nil {
		return vterrors.Wrapf(err, "VReplication(start until) for %v until %v failed", shardInfo.MasterAlias, mysqlPos)
	}
	if err := tabletManagerClient.VReplicationWaitForPos(shortCtx, masterInfo.Tablet, int(ss.Uid), mysqlPos); err != nil {
		return vterrors.Wrapf(err, "VReplicationWaitForPos for %v until %v failed", shardInfo.MasterAlias, mysqlPos)
	}
	masterPos, err := tabletManagerClient.MasterPosition(shortCtx, masterInfo.Tablet)
	if err != nil {
		return vterrors.Wrapf(err, "MasterPosition for %v failed", shardInfo.MasterAlias)
	}

	// 4 - wait until the destination tablet is equal or passed
	//     that master binlog position, and stop its replication.
	prd.logger.Infof("Waiting for destination tablet %v to catch up to %v", topoproto.TabletAliasString(destinationAlias), masterPos)
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	defer cancel()
	destinationTablet, err := toposerver.GetTablet(shortCtx, destinationAlias)
	if err != nil {
		return err
	}
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	defer cancel()
	_, err = tabletManagerClient.StopSlaveMinimum(shortCtx, destinationTablet.Tablet, masterPos, *remoteActionsTimeout)
	if err != nil {
		return vterrors.Wrapf(err, "StopSlaveMinimum on %v at %v failed", topoproto.TabletAliasString(destinationAlias), masterPos)
	}
	wrangler.RecordStartSlaveAction(prd.cleaner, destinationTablet.Tablet)

	// 5 - restart filtered replication on destination master
	prd.logger.Infof("Restarting filtered replication on master %v", topoproto.TabletAliasString(shardInfo.MasterAlias))
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	defer cancel()
	if _, err = tabletManagerClient.VReplicationExec(ctx, masterInfo.Tablet, binlogplayer.StartVReplication(ss.Uid)); err != nil {
		return vterrors.Wrapf(err, "VReplicationExec(start) failed for %v", shardInfo.MasterAlias)
	}

	return nil
}

// diff phase: will create a list of messages regarding the diff.
// - get the schema on all tablets
// - if some table schema mismatches, record them (use existing schema diff tools).
// - for each table in destination, run a diff pipeline.
func (prd *PausedReplicationDiffer) Diff(ctx context.Context,
	wr *wrangler.Wrangler,
	sourceAlias *topodatapb.TabletAlias,
	destinationAlias *topodatapb.TabletAlias,
	shardInfo *topo.ShardInfo,
	markAsWillFail func(rec concurrency.ErrorRecorder, err error),
	parallelDiffsCount int) error {
	prd.logger.Infof("Gathering schema information...")
	wg := sync.WaitGroup{}
	rec := &concurrency.AllErrorRecorder{}

	var destinationSchemaDefinition *tabletmanagerdatapb.SchemaDefinition
	var sourceSchemaDefinition *tabletmanagerdatapb.SchemaDefinition

	wg.Add(1)
	go func() {
		var err error
		shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
		destinationSchemaDefinition, err = wr.GetSchema(
			shortCtx, destinationAlias, shardInfo.SourceShards[0].Tables, nil /* excludeTables */, false /* includeViews */)
		cancel()
		if err != nil {
			markAsWillFail(rec, err)
		}
		prd.logger.Infof("Got schema from destination %v", topoproto.TabletAliasString(destinationAlias))
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		var err error
		shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
		sourceSchemaDefinition, err = wr.GetSchema(
			shortCtx, sourceAlias, shardInfo.SourceShards[0].Tables, nil /* excludeTables */, false /* includeViews */)
		cancel()
		if err != nil {
			markAsWillFail(rec, err)
		}
		prd.logger.Infof("Got schema from source %v", topoproto.TabletAliasString(sourceAlias))
		wg.Done()
	}()
	wg.Wait()
	if rec.HasErrors() {
		return rec.Error()
	}

	// Check the schema
	prd.logger.Infof("Diffing the schema...")
	rec = &concurrency.AllErrorRecorder{}
	tmutils.DiffSchema("destination", destinationSchemaDefinition, "source", sourceSchemaDefinition, rec)
	if rec.HasErrors() {
		prd.logger.Warningf("Different schemas: %v", rec.Error())
	} else {
		prd.logger.Infof("Schema match, good.")
	}

	// run the diffs, 8 at a time
	prd.logger.Infof("Running the diffs...")
	sem := sync2.NewSemaphore(parallelDiffsCount, 0)
	for _, tableDefinition := range destinationSchemaDefinition.TableDefinitions {
		wg.Add(1)
		go func(tableDefinition *tabletmanagerdatapb.TableDefinition) {
			defer wg.Done()
			sem.Acquire()
			defer sem.Release()

			prd.logger.Infof("Starting the diff on table %v", tableDefinition.Name)
			sourceQueryResultReader, err := TableScan(ctx, prd.logger, wr.TopoServer(), sourceAlias, tableDefinition)
			if err != nil {
				newErr := vterrors.Wrap(err, "TableScan(source) failed")
				markAsWillFail(rec, newErr)
				prd.logger.Error(newErr)
				return
			}
			defer sourceQueryResultReader.Close(ctx)

			destinationQueryResultReader, err := TableScan(ctx, prd.logger, wr.TopoServer(), destinationAlias, tableDefinition)
			if err != nil {
				newErr := vterrors.Wrap(err, "TableScan(destination) failed")
				markAsWillFail(rec, newErr)
				prd.logger.Error(newErr)
				return
			}
			defer destinationQueryResultReader.Close(ctx)

			differ, err := NewRowDiffer(sourceQueryResultReader, destinationQueryResultReader, tableDefinition)
			if err != nil {
				newErr := vterrors.Wrap(err, "NewRowDiffer() failed")
				markAsWillFail(rec, newErr)
				prd.logger.Error(newErr)
				return
			}

			report, err := differ.Go(prd.logger)
			if err != nil {
				prd.logger.Errorf2(err, "Differ.Go failed")
			} else {
				if report.HasDifferences() {
					err := fmt.Errorf("table %v has differences: %v", tableDefinition.Name, report.String())
					markAsWillFail(rec, err)
					prd.logger.Error(err)
				} else {
					prd.logger.Infof("Table %v checks out (%v rows processed, %v qps)", tableDefinition.Name, report.processedRows, report.processingQPS)
				}
			}
		}(tableDefinition)
	}
	wg.Wait()

	return rec.Error()
}
