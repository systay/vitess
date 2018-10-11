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

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/wrangler"

	"sort"

	"bytes"

	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/key"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// MultiSplitDiffWorker executes a diff between a destination shard and its
// source shards in a shard split case.
type MultiSplitDiffWorker struct {
	StatusWorker

	wr                                *wrangler.Wrangler
	cell                              string
	keyspace                          string
	shard                             string
	excludeTables                     []string
	excludeShards                     []string
	minHealthyRdonlyTablets           int
	destinationTabletType             topodatapb.TabletType
	parallelDiffsCount                int
	waitForFixedTimeRatherThanGtidSet bool
	cleaner                           *wrangler.Cleaner

	// populated during WorkerStateInit, read-only after that
	keyspaceInfo      *topo.KeyspaceInfo
	shardInfo         *topo.ShardInfo
	sourceUID         uint32
	destinationShards []*topo.ShardInfo

	// populated during WorkerStateFindTargets, read-only after that
	sourceAlias        *topodatapb.TabletAlias
	destinationAliases []*topodatapb.TabletAlias // matches order of destinationShards

	// populated during WorkerStateDiff
	sourceSchemaDefinition       *tabletmanagerdatapb.SchemaDefinition
	destinationSchemaDefinitions []*tabletmanagerdatapb.SchemaDefinition
}

// NewMultiSplitDiffWorker returns a new MultiSplitDiffWorker object.
func NewMultiSplitDiffWorker(wr *wrangler.Wrangler, cell, keyspace, shard string, excludeTables []string, excludeShards []string, minHealthyRdonlyTablets, parallelDiffsCount int, waitForFixedTimeRatherThanGtidSet bool, tabletType topodatapb.TabletType) Worker {
	return &MultiSplitDiffWorker{
		StatusWorker:                      NewStatusWorker(),
		wr:                                wr,
		cell:                              cell,
		keyspace:                          keyspace,
		shard:                             shard,
		excludeTables:                     excludeTables,
		excludeShards:                     excludeShards,
		minHealthyRdonlyTablets:           minHealthyRdonlyTablets,
		destinationTabletType:             tabletType,
		parallelDiffsCount:                parallelDiffsCount,
		waitForFixedTimeRatherThanGtidSet: waitForFixedTimeRatherThanGtidSet,
		cleaner: &wrangler.Cleaner{},
	}
}

// StatusAsHTML is part of the Worker interface
func (msdw *MultiSplitDiffWorker) StatusAsHTML() template.HTML {
	state := msdw.State()

	result := "<b>Working on:</b> " + msdw.keyspace + "/" + msdw.shard + "</br>\n"
	result += "<b>State:</b> " + state.String() + "</br>\n"
	switch state {
	case WorkerStateDiff:
		result += "<b>Running...</b></br>\n"
	case WorkerStateDone:
		result += "<b>Success.</b></br>\n"
	}

	return template.HTML(result)
}

// StatusAsText is part of the Worker interface
func (msdw *MultiSplitDiffWorker) StatusAsText() string {
	state := msdw.State()

	result := "Working on: " + msdw.keyspace + "/" + msdw.shard + "\n"
	result += "State: " + state.String() + "\n"
	switch state {
	case WorkerStateDiff:
		result += "Running...\n"
	case WorkerStateDone:
		result += "Success.\n"
	}
	return result
}

// Run is mostly a wrapper to run the cleanup at the end.
func (msdw *MultiSplitDiffWorker) Run(ctx context.Context) error {
	resetVars()
	err := msdw.run(ctx)

	msdw.SetState(WorkerStateCleanUp)
	cerr := msdw.cleaner.CleanUp(msdw.wr)
	if cerr != nil {
		if err != nil {
			msdw.wr.Logger().Errorf("CleanUp failed in addition to job error: %v", cerr)
		} else {
			err = cerr
		}
	}
	if err != nil {
		msdw.wr.Logger().Errorf("Run() error: %v", err)
		msdw.SetState(WorkerStateError)
		return err
	}
	msdw.SetState(WorkerStateDone)
	return nil
}

func (msdw *MultiSplitDiffWorker) run(ctx context.Context) error {
	// first state: read what we need to do
	if err := msdw.init(ctx); err != nil {
		return fmt.Errorf("init() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// second state: find targets
	if err := msdw.findTargets(ctx); err != nil {
		return fmt.Errorf("findTargets() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// third phase: synchronize replication
	if err := msdw.synchronizeReplication(ctx); err != nil {
		return fmt.Errorf("synchronizeReplication() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// fourth phase: diff
	if err := msdw.diff(ctx); err != nil {
		return fmt.Errorf("diff() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	return nil
}

func (msdw *MultiSplitDiffWorker) searchInKeyspace(ctx context.Context, wg *sync.WaitGroup, rec *concurrency.AllErrorRecorder, keyspace string, result chan *topo.ShardInfo, uids chan uint32) {
	defer wg.Done()
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	shards, err := msdw.wr.TopoServer().GetShardNames(shortCtx, keyspace)
	cancel()
	if err != nil {
		rec.RecordError(fmt.Errorf("failed to get list of shards for keyspace '%v': %v", keyspace, err))
		return
	}
	for _, shard := range shards {
		wg.Add(1)
		go msdw.produceShardInfo(ctx, wg, rec, keyspace, shard, result, uids)
	}
}

func (msdw *MultiSplitDiffWorker) produceShardInfo(ctx context.Context, wg *sync.WaitGroup, rec *concurrency.AllErrorRecorder, keyspace string, shard string, result chan *topo.ShardInfo, uids chan uint32) {
	defer wg.Done()
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	si, err := msdw.wr.TopoServer().GetShard(shortCtx, keyspace, shard)
	cancel()
	if err != nil {
		rec.RecordError(fmt.Errorf("failed to get details for shard '%v': %v", topoproto.KeyspaceShardString(keyspace, shard), err))
		return
	}

	if stringContains(msdw.excludeShards, si.ShardName()) {
		msdw.wr.Logger().Infof("ignoring shard %v/%v", si.Keyspace(), si.ShardName())
		return
	}

	for _, sourceShard := range si.SourceShards {
		if len(sourceShard.Tables) == 0 && sourceShard.Keyspace == msdw.keyspace && sourceShard.Shard == msdw.shard {
			result <- si
			uids <- sourceShard.Uid
			// Prevents the same shard from showing up multiple times
			return
		}
	}
}

// findDestinationShards finds all the shards that have filtered replication from the source shard
func (msdw *MultiSplitDiffWorker) findDestinationShards(ctx context.Context) ([]*topo.ShardInfo, error) {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	keyspaces, err := msdw.wr.TopoServer().GetKeyspaces(shortCtx)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("failed to get list of keyspaces: %v", err)
	}

	producers := sync.WaitGroup{}
	consumer := sync.WaitGroup{}
	result := make(chan *topo.ShardInfo, 2 /*the consumer will just copy out the data, so this should be enough*/)
	sourceUIDs := make(chan uint32, 2 /*the consumer will just copy out the data, so this should be enough*/)
	rec := concurrency.AllErrorRecorder{}
	var resultArray = make([]*topo.ShardInfo, 0, len(keyspaces))

	for _, keyspace := range keyspaces {
		producers.Add(1)
		go msdw.searchInKeyspace(ctx, &producers, &rec, keyspace, result, sourceUIDs)
	}

	// Start the result array consumer
	consumer.Add(1)
	go func() {
		defer consumer.Done()
		for r := range result {
			resultArray = append(resultArray, r)
		}
	}()

	// Start the sourceUID check consumer
	consumer.Add(1)
	go func() {
		defer consumer.Done()
		first := true
		for r := range sourceUIDs {
			if first {
				first = false
				msdw.sourceUID = r
			} else if r != msdw.sourceUID {
				rec.RecordError(fmt.Errorf("found a source ID that was different, aborting. %v vs %v", r, msdw.sourceUID))
			}
		}
	}()

	// we use this pattern because we don't know the size of shards up front, so we are using a buffered channel
	producers.Wait()
	close(result)
	close(sourceUIDs)
	consumer.Wait()

	if rec.HasErrors() {
		return nil, rec.Error()
	}

	if len(resultArray) == 0 {
		return nil, fmt.Errorf("there are no destination shards")
	}
	return resultArray, nil
}

func stringContains(l []string, s string) bool {
	for _, v := range l {
		if v == s {
			return true
		}
	}
	return false
}

// init phase:
// - read the shard info, make sure it has sources
func (msdw *MultiSplitDiffWorker) init(ctx context.Context) error {
	msdw.SetState(WorkerStateInit)

	var err error
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	msdw.keyspaceInfo, err = msdw.wr.TopoServer().GetKeyspace(shortCtx, msdw.keyspace)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot read keyspace %v: %v", msdw.keyspace, err)
	}
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	msdw.shardInfo, err = msdw.wr.TopoServer().GetShard(shortCtx, msdw.keyspace, msdw.shard)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot read shard %v/%v: %v", msdw.keyspace, msdw.shard, err)
	}

	if !msdw.shardInfo.HasMaster() {
		return fmt.Errorf("shard %v/%v has no master", msdw.keyspace, msdw.shard)
	}

	destinationShards, err := msdw.findDestinationShards(ctx)
	if err != nil {
		return fmt.Errorf("findDestinationShards() failed for %v/%v/%v: %v", msdw.cell, msdw.keyspace, msdw.shard, err)
	}
	msdw.destinationShards = destinationShards

	return nil
}

// findTargets phase:
// - find one rdonly in source shard
// - find one rdonly per destination shard
// - mark them all as 'worker' pointing back to us
func (msdw *MultiSplitDiffWorker) findTargets(ctx context.Context) error {
	msdw.SetState(WorkerStateFindTargets)

	var err error

	// find an appropriate tablet in the source shard
	msdw.sourceAlias, err = FindWorkerTablet(
		ctx,
		msdw.wr,
		msdw.cleaner,
		nil, /* tsc */
		msdw.cell,
		msdw.keyspace,
		msdw.shard,
		1, /* minHealthyTablets */
		msdw.destinationTabletType)
	if err != nil {
		return fmt.Errorf("FindWorkerTablet() failed for %v/%v/%v: %v", msdw.cell, msdw.keyspace, msdw.shard, err)
	}

	// find an appropriate tablet in each destination shard
	msdw.destinationAliases = make([]*topodatapb.TabletAlias, len(msdw.destinationShards))
	for i, destinationShard := range msdw.destinationShards {
		keyspace := destinationShard.Keyspace()
		shard := destinationShard.ShardName()
		destinationAlias, err := FindWorkerTablet(
			ctx,
			msdw.wr,
			msdw.cleaner,
			nil, /* tsc */
			msdw.cell,
			keyspace,
			shard,
			msdw.minHealthyRdonlyTablets,
			topodatapb.TabletType_RDONLY)
		if err != nil {
			return fmt.Errorf("FindWorkerTablet() failed for %v/%v/%v: %v", msdw.cell, keyspace, shard, err)
		}
		msdw.destinationAliases[i] = destinationAlias
	}
	if err != nil {
		return fmt.Errorf("FindWorkerTablet() failed for %v/%v/%v: %v", msdw.cell, msdw.keyspace, msdw.shard, err)
	}

	return nil
}

// ask the master of the destination shard to pause filtered replication,
// and return the source binlog positions
// (add a cleanup task to restart filtered replication on master)
func (msdw *MultiSplitDiffWorker) stopReplicationOnAllDestinationMasters(ctx context.Context, masterInfos []*topo.TabletInfo) ([]string, error) {
	destVreplicationPos := make([]string, len(msdw.destinationShards))

	for i, shardInfo := range msdw.destinationShards {
		masterInfo := masterInfos[i]

		msdw.wr.Logger().Infof("Stopping master binlog replication on %v", shardInfo.MasterAlias)
		shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
		_, err := msdw.wr.TabletManagerClient().VReplicationExec(shortCtx, masterInfo.Tablet, binlogplayer.StopVReplication(msdw.sourceUID, "for split diff"))
		msdw.wr.Logger().Infof("using sourceUID %v", msdw.sourceUID)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("VReplicationExec(stop) for %v failed: %v", shardInfo.MasterAlias, err)
		}
		wrangler.RecordVReplicationAction(msdw.cleaner, masterInfo.Tablet, binlogplayer.StartVReplication(msdw.sourceUID))
		shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
		p3qr, err := msdw.wr.TabletManagerClient().VReplicationExec(shortCtx, masterInfo.Tablet, binlogplayer.ReadVReplicationPos(msdw.sourceUID))
		cancel()
		if err != nil {
			return nil, fmt.Errorf("VReplicationExec(stop) for %v failed: %v", msdw.shardInfo.MasterAlias, err)
		}
		qr := sqltypes.Proto3ToResult(p3qr)
		if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
			return nil, fmt.Errorf("unexpected result while reading position: %v", qr)
		}
		destVreplicationPos[i] = qr.Rows[0][0].ToString()
		if err != nil {
			return nil, fmt.Errorf("StopBlp for %v failed: %v", msdw.shardInfo.MasterAlias, err)
		}
	}
	return destVreplicationPos, nil
}

func (msdw *MultiSplitDiffWorker) getTabletInfoForShard(ctx context.Context, shardInfo *topo.ShardInfo) (*topo.TabletInfo, error) {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	masterInfo, err := msdw.wr.TopoServer().GetTablet(shortCtx, shardInfo.MasterAlias)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("synchronizeReplication: cannot get Tablet record for master %v: %v", msdw.shardInfo.MasterAlias, err)
	}
	return masterInfo, nil
}

//  stop the source tablet at a binlog position higher than the
//  destination masters. Return the reached position
//  (add a cleanup task to restart binlog replication on the source tablet, and
//   change the existing ChangeSlaveType cleanup action to 'spare' type)
func (msdw *MultiSplitDiffWorker) stopReplicationOnSourceRdOnlyTabletAt(ctx context.Context, destVreplicationPos []string) (string, error) {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	sourceTablet, err := msdw.wr.TopoServer().GetTablet(shortCtx, msdw.sourceAlias)
	cancel()
	if err != nil {
		return "", err
	}

	var mysqlPos string // will be the last GTID that we stopped at
	for _, vreplicationPos := range destVreplicationPos {
		// We need to stop the source RDONLY tablet at a position which includes ALL of the positions of the destination
		// shards. We do this by starting replication and then stopping at a minimum of each blp position separately.
		// TODO this is not terribly efficient but it's possible to implement without changing the existing RPC,
		// if we make StopSlaveMinimum take multiple blp positions then this will be a lot more efficient because you just
		// check for each position using WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS and then stop replication.

		msdw.wr.Logger().Infof("Stopping slave %v at a minimum of %v", msdw.sourceAlias, vreplicationPos)
		// read the tablet
		sourceTablet, err := msdw.wr.TopoServer().GetTablet(shortCtx, msdw.sourceAlias)
		if err != nil {
			return "", err
		}
		shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
		msdw.wr.TabletManagerClient().StartSlave(shortCtx, sourceTablet.Tablet)
		cancel()
		if err != nil {
			return "", err
		}

		shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
		mysqlPos, err = msdw.wr.TabletManagerClient().StopSlaveMinimum(shortCtx, sourceTablet.Tablet, vreplicationPos, *remoteActionsTimeout)
		cancel()
		if err != nil {
			return "", fmt.Errorf("cannot stop slave %v at right binlog position %v: %v", msdw.sourceAlias, vreplicationPos, err)
		}
	}
	// change the cleaner actions from ChangeSlaveType(rdonly)
	// to StartSlave() + ChangeSlaveType(spare)
	wrangler.RecordStartSlaveAction(msdw.cleaner, sourceTablet.Tablet)

	return mysqlPos, nil
}

// ask the master of the destination shard to resume filtered replication
// up to the new list of positions, and return its binlog position.
func (msdw *MultiSplitDiffWorker) resumeReplicationOnDestinationMasterUntil(ctx context.Context, shardInfo *topo.ShardInfo, mysqlPos string, masterInfo *topo.TabletInfo) (string, error) {
	msdw.wr.Logger().Infof("Restarting master %v until it catches up to %v", shardInfo.MasterAlias, mysqlPos)
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	_, err := msdw.wr.TabletManagerClient().VReplicationExec(shortCtx, masterInfo.Tablet, binlogplayer.StartVReplicationUntil(msdw.sourceUID, mysqlPos))
	cancel()
	if err != nil {
		return "", fmt.Errorf("VReplication(start until) for %v until %v failed: %v", shardInfo.MasterAlias, mysqlPos, err)
	}
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	if err := msdw.wr.TabletManagerClient().VReplicationWaitForPos(shortCtx, masterInfo.Tablet, int(msdw.sourceUID), mysqlPos); err != nil {
		cancel()
		return "", fmt.Errorf("VReplicationWaitForPos for %v until %v failed: %v", shardInfo.MasterAlias, mysqlPos, err)
	}
	cancel()

	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	masterPos, err := msdw.wr.TabletManagerClient().MasterPosition(shortCtx, masterInfo.Tablet)
	cancel()
	if err != nil {
		return "", fmt.Errorf("MasterPosition for %v failed: %v", msdw.shardInfo.MasterAlias, err)
	}
	return masterPos, nil
}

// wait until the destination tablet is equal or passed that master
// binlog position, and stop its replication.
// (add a cleanup task to restart binlog replication on it, and change
//  the existing ChangeSlaveType cleanup action to 'spare' type)
func (msdw *MultiSplitDiffWorker) stopReplicationOnDestinationRdOnlys(ctx context.Context, destinationAlias *topodatapb.TabletAlias, masterPos string) error {
	if msdw.waitForFixedTimeRatherThanGtidSet {
		msdw.wr.Logger().Infof("Workaround for broken GTID set in destination RDONLY. Just waiting for 1 minute for %v and assuming replication has caught up. (should be at %v)", destinationAlias, masterPos)
	} else {
		msdw.wr.Logger().Infof("Waiting for destination tablet %v to catch up to %v", destinationAlias, masterPos)
	}
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	destinationTablet, err := msdw.wr.TopoServer().GetTablet(shortCtx, destinationAlias)
	cancel()
	if err != nil {
		return err
	}

	if msdw.waitForFixedTimeRatherThanGtidSet {
		time.Sleep(1 * time.Minute)
	}

	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	if msdw.waitForFixedTimeRatherThanGtidSet {
		err = msdw.wr.TabletManagerClient().StopSlave(shortCtx, destinationTablet.Tablet)
	} else {
		_, err = msdw.wr.TabletManagerClient().StopSlaveMinimum(shortCtx, destinationTablet.Tablet, masterPos, *remoteActionsTimeout)
	}
	cancel()
	if err != nil {
		return fmt.Errorf("StopSlaveMinimum for %v at %v failed: %v", destinationAlias, masterPos, err)
	}
	wrangler.RecordStartSlaveAction(msdw.cleaner, destinationTablet.Tablet)
	return nil
}

// restart filtered replication on the destination master.
// (remove the cleanup task that does the same)
func (msdw *MultiSplitDiffWorker) restartReplicationOn(ctx context.Context, shardInfo *topo.ShardInfo, masterInfo *topo.TabletInfo) error {
	msdw.wr.Logger().Infof("Restarting filtered replication on master %v", shardInfo.MasterAlias)
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	_, err := msdw.wr.TabletManagerClient().VReplicationExec(shortCtx, masterInfo.Tablet, binlogplayer.StartVReplication(msdw.sourceUID))
	if err != nil {
		return fmt.Errorf("VReplicationExec(start) failed for %v: %v", shardInfo.MasterAlias, err)
	}
	cancel()
	return nil
}

// synchronizeReplication phase:
// At this point, the source and the destination tablet are stopped at the same
// point.

func (msdw *MultiSplitDiffWorker) synchronizeReplication(ctx context.Context) error {
	msdw.SetState(WorkerStateSyncReplication)
	var err error

	masterInfos := make([]*topo.TabletInfo, len(msdw.destinationAliases))
	for i, shardInfo := range msdw.destinationShards {
		masterInfos[i], err = msdw.getTabletInfoForShard(ctx, shardInfo)
		if err != nil {
			return err
		}
	}

	destVreplicationPos, err := msdw.stopReplicationOnAllDestinationMasters(ctx, masterInfos)
	if err != nil {
		return err
	}

	mysqlPos, err := msdw.stopReplicationOnSourceRdOnlyTabletAt(ctx, destVreplicationPos)
	if err != nil {
		return err
	}

	for i, shardInfo := range msdw.destinationShards {
		masterInfo := masterInfos[i]
		destinationAlias := msdw.destinationAliases[i]

		masterPos, err := msdw.resumeReplicationOnDestinationMasterUntil(ctx, shardInfo, mysqlPos, masterInfo)
		if err != nil {
			return err
		}

		err = msdw.stopReplicationOnDestinationRdOnlys(ctx, destinationAlias, masterPos)
		if err != nil {
			return err
		}

		err = msdw.restartReplicationOn(ctx, shardInfo, masterInfo)
		if err != nil {
			return err
		}
	}

	return nil
}

// diff phase: will log messages regarding the diff.
// - get the schema on all tablets
// - if some table schema mismatches, record them (use existing schema diff tools).
// - for each table in destination, run a diff pipeline.

func (msdw *MultiSplitDiffWorker) diff(ctx context.Context) error {
	msdw.SetState(WorkerStateDiff)

	msdw.wr.Logger().Infof("Gathering schema information...")
	wg := sync.WaitGroup{}
	rec := &concurrency.AllErrorRecorder{}
	mu := sync.Mutex{} // protects msdw.destinationSchemaDefinitions
	msdw.destinationSchemaDefinitions = make([]*tabletmanagerdatapb.SchemaDefinition, len(msdw.destinationAliases))
	for i, destinationAlias := range msdw.destinationAliases {
		wg.Add(1)
		go func(i int, destinationAlias *topodatapb.TabletAlias) {
			var err error
			shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
			destinationSchemaDefinition, err := msdw.wr.GetSchema(
				shortCtx, destinationAlias, nil /* tables */, msdw.excludeTables, false /* includeViews */)
			cancel()
			rec.RecordError(err)
			mu.Lock()
			msdw.destinationSchemaDefinitions[i] = destinationSchemaDefinition
			mu.Unlock()
			msdw.wr.Logger().Infof("Got schema from destination %v", destinationAlias)
			wg.Done()
		}(i, destinationAlias)
	}
	wg.Add(1)
	go func() {
		var err error
		shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
		msdw.sourceSchemaDefinition, err = msdw.wr.GetSchema(
			shortCtx, msdw.sourceAlias, nil /* tables */, msdw.excludeTables, false /* includeViews */)
		cancel()
		rec.RecordError(err)
		msdw.wr.Logger().Infof("Got schema from source %v", msdw.sourceAlias)
		wg.Done()
	}()

	wg.Wait()
	if rec.HasErrors() {
		return rec.Error()
	}

	msdw.wr.Logger().Infof("Diffing the schema...")
	rec = &concurrency.AllErrorRecorder{}
	sourceShardName := fmt.Sprintf("%v/%v", msdw.shardInfo.Keyspace(), msdw.shardInfo.ShardName())
	for i, destinationSchemaDefinition := range msdw.destinationSchemaDefinitions {
		destinationShard := msdw.destinationShards[i]
		destinationShardName := fmt.Sprintf("%v/%v", destinationShard.Keyspace(), destinationShard.ShardName())
		tmutils.DiffSchema(destinationShardName, destinationSchemaDefinition, sourceShardName, msdw.sourceSchemaDefinition, rec)
	}
	if rec.HasErrors() {
		msdw.wr.Logger().Warningf("Different schemas: %v", rec.Error().Error())
	} else {
		msdw.wr.Logger().Infof("Schema match, good.")
	}

	// read the vschema if needed
	var keyspaceSchema *vindexes.KeyspaceSchema
	if *useV3ReshardingMode {
		kschema, err := msdw.wr.TopoServer().GetVSchema(ctx, msdw.keyspace)
		if err != nil {
			return fmt.Errorf("cannot load VSchema for keyspace %v: %v", msdw.keyspace, err)
		}
		if kschema == nil {
			return fmt.Errorf("no VSchema for keyspace %v", msdw.keyspace)
		}

		keyspaceSchema, err = vindexes.BuildKeyspaceSchema(kschema, msdw.keyspace)
		if err != nil {
			return fmt.Errorf("cannot build vschema for keyspace %v: %v", msdw.keyspace, err)
		}
	}

	// Compute the overlap keyrange. Later, we'll compare it with
	// source or destination keyrange. If it matches either,
	// we'll just ask for all the data. If the overlap is a subset,
	// we'll filter.
	var err error
	var keyranges = make([]*topodatapb.KeyRange, len(msdw.destinationShards))
	for i, destinationShard := range msdw.destinationShards {
		keyranges[i] = destinationShard.KeyRange
	}
	union, err := keyRangesUnion(keyranges)
	if err != nil {
		return err
	}

	// run diffs in parallel
	msdw.wr.Logger().Infof("Running the diffs...")
	sem := sync2.NewSemaphore(msdw.parallelDiffsCount, 0)
	tableDefinitions := msdw.sourceSchemaDefinition.TableDefinitions

	// sort tables by size
	// if there are large deltas between table sizes then it's more efficient to start working on the large tables first
	sort.Slice(tableDefinitions, func(i, j int) bool { return tableDefinitions[i].DataLength > tableDefinitions[j].DataLength })

	// use a channel to make sure tables are diffed in order
	tableChan := make(chan *tabletmanagerdatapb.TableDefinition, len(tableDefinitions))
	for _, tableDefinition := range tableDefinitions {
		tableChan <- tableDefinition
	}

	// start as many goroutines as there are tables to diff
	for range tableDefinitions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// use the semaphore to limit the number of tables that are diffed in parallel
			sem.Acquire()
			defer sem.Release()

			// grab the table to process out of the channel
			tableDefinition := <-tableChan

			msdw.wr.Logger().Infof("Starting the diff on table %v", tableDefinition.Name)

			// On the source, see if we need a full scan
			// or a filtered scan.
			var err error
			var sourceQueryResultReader *QueryResultReader
			if key.KeyRangeEqual(union, msdw.shardInfo.KeyRange) {
				sourceQueryResultReader, err = TableScan(ctx, msdw.wr.Logger(), msdw.wr.TopoServer(), msdw.sourceAlias, tableDefinition)
			} else {
				msdw.wr.Logger().Infof("Filtering source down to %v", union)
				sourceQueryResultReader, err = TableScanByKeyRange(ctx, msdw.wr.Logger(), msdw.wr.TopoServer(), msdw.sourceAlias, tableDefinition, union, keyspaceSchema, msdw.keyspaceInfo.ShardingColumnName, msdw.keyspaceInfo.ShardingColumnType)
			}
			if err != nil {
				newErr := fmt.Errorf("TableScan(source) failed: %v", err)
				rec.RecordError(newErr)
				msdw.wr.Logger().Errorf("%v", newErr)
				return
			}
			defer sourceQueryResultReader.Close(ctx)

			// On the destination, see if we need a full scan
			// or a filtered scan.
			destinationQueryResultReaders := make([]ResultReader, len(msdw.destinationAliases))
			for i, destinationAlias := range msdw.destinationAliases {
				destinationQueryResultReader, err := TableScan(ctx, msdw.wr.Logger(), msdw.wr.TopoServer(), destinationAlias, tableDefinition)
				if err != nil {
					newErr := fmt.Errorf("TableScan(destination) failed: %v", err)
					rec.RecordError(newErr)
					msdw.wr.Logger().Errorf("%v", newErr)
					return
				}
				//noinspection GoDeferInLoop
				defer destinationQueryResultReader.Close(ctx)
				destinationQueryResultReaders[i] = destinationQueryResultReader
			}
			mergedResultReader, err := NewResultMerger(destinationQueryResultReaders, len(tableDefinition.PrimaryKeyColumns))
			if err != nil {
				newErr := fmt.Errorf("NewResultMerger failed: %v", err)
				rec.RecordError(newErr)
				msdw.wr.Logger().Errorf("%v", newErr)
				return
			}

			// Create the row differ.
			differ, err := NewRowDiffer(sourceQueryResultReader, mergedResultReader, tableDefinition)
			if err != nil {
				newErr := fmt.Errorf("NewRowDiffer() failed: %v", err)
				rec.RecordError(newErr)
				msdw.wr.Logger().Errorf("%v", newErr)
				return
			}

			// And run the diff.
			report, err := differ.Go(msdw.wr.Logger())
			if err != nil {
				newErr := fmt.Errorf("Differ.Go failed: %v", err.Error())
				rec.RecordError(newErr)
				msdw.wr.Logger().Errorf("%v", newErr)
			} else {
				if report.HasDifferences() {
					err := fmt.Errorf("table %v has differences: %v", tableDefinition.Name, report.String())
					rec.RecordError(err)
					msdw.wr.Logger().Warningf(err.Error())
				} else {
					msdw.wr.Logger().Infof("Table %v checks out (%v rows processed, %v qps)", tableDefinition.Name, report.processedRows, report.processingQPS)
				}
			}
		}()
	}

	// grab the table to process out of the channel
	wg.Wait()

	return rec.Error()
}

func keyRangesUnion(keyranges []*topodatapb.KeyRange) (*topodatapb.KeyRange, error) {
	// HACK HACK HACK
	// This assumes the ranges are consecutive. It just returns the smallest start and the largest end.

	var start []byte
	var end []byte
	for i, keyrange := range keyranges {
		if i == 0 {
			// initialize the first values
			start = keyrange.Start
			end = keyrange.End
			continue
		}

		// nil always wins
		if keyrange.Start == nil {
			start = nil
		} else {
			if start != nil {
				if bytes.Compare(start, keyrange.Start) > 0 {
					start = keyrange.Start
				}
			}
		}

		if keyrange.End == nil {
			end = nil
		} else {
			if end != nil {
				if bytes.Compare(end, keyrange.End) < 0 {
					end = keyrange.End
				}
			}
		}
	}

	return &topodatapb.KeyRange{Start: start, End: end}, nil
}
