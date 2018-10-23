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
	"errors"
	"fmt"
	"html/template"
	"sync"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/proto/tabletmanagerdata"

	// This cmd imports consultopo to register the consul implementation of TopoServer.
	_ "vitess.io/vitess/go/vt/topo/consultopo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	// This cmd imports zk2topo to register the zk2 implementation of TopoServer.
	_ "vitess.io/vitess/go/vt/topo/zk2topo"
	// We will use gRPC to connect, register the dialer
	_ "vitess.io/vitess/go/vt/vttablet/grpctabletconn"
	// import the gRPC client implementation for tablet manager
	_ "vitess.io/vitess/go/vt/vttablet/grpctmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	"vitess.io/vitess/go/vt/proto/topodata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/wrangler"
)

// TabletDiffWorker executes a diff between a two tablets in the same shard using replication
// pausing and consistent snapshot transactions to do everything on a live system
type TabletDiffWorker struct {
	StatusWorker
	// These fields are set at creation time and does not change
	wr               *wrangler.Wrangler
	sourceTablet     string
	destTablet       string
	cleaner          *wrangler.Cleaner
	concurrentTables int64
	excludedTables   []string

	// populated during WorkerStateInit, read-only after that
	source      *topo.TabletInfo
	destination *topo.TabletInfo
	destAlias   *topodata.TabletAlias
	sourceAlias *topodatapb.TabletAlias

	// populated during WorkerStateSyncReplication, read-only after that
	connections  []TableScanner
	executedGtid string
}

// NewTabletDiffWorker returns a new TabletDiffWorker
func NewTabletDiffWorker(wr *wrangler.Wrangler, srcTablet, dstTablet string, parallelDiffsCount int64, excludedTablets []string, cleaner *wrangler.Cleaner) Worker {
	return &TabletDiffWorker{
		StatusWorker:     NewStatusWorker(),
		wr:               wr,
		sourceTablet:     srcTablet,
		destTablet:       dstTablet,
		cleaner:          cleaner,
		concurrentTables: parallelDiffsCount,
		excludedTables:   excludedTablets,
	}
}

// StatusAsHTML is part of the Worker interface
func (mdw *TabletDiffWorker) StatusAsHTML() template.HTML {
	state := mdw.State()

	result := "<b>Table diffing between :</b> " + mdw.sourceTablet + " and " + mdw.destTablet + "</br>\n"
	result += "<b>State:</b> " + state.String() + "</br>\n"
	switch state {
	case WorkerStateDiff:
		result += "<b>Running...</b></br>\n"
	case WorkerStateDone:
		result += "<b>Success.</b></br>\n"
	case WorkerStateError:
		result += "<b>Error.</b></br>\n"
	}

	return template.HTML(result)
}

// StatusAsText is part of the Worker interface
func (mdw *TabletDiffWorker) StatusAsText() string {
	state := mdw.State()

	result := "Table diffing between : [" + mdw.sourceTablet + "] and [" + mdw.destTablet + "]\n"
	result += "State: " + state.String() + "\n"
	switch state {
	case WorkerStateDiff:
		result += "Running...\n"
	case WorkerStateDone:
		result += "Success.\n"
	case WorkerStateError:
		result += "Error.\n"
	}
	return result
}

// Run is mostly a wrapper to run the cleanup at the end.
func (mdw *TabletDiffWorker) Run(ctx context.Context) error {
	resetVars()
	runError := mdw.run(ctx)

	mdw.SetState(WorkerStateCleanUp)
	cleanUpError := mdw.cleaner.CleanUp(mdw.wr)
	if cleanUpError != nil {
		if runError != nil {
			mdw.wr.Logger().Errorf("CleanUp failed in addition to job error: %v", cleanUpError)
		} else {
			runError = cleanUpError
		}
	}
	if runError == nil {
		mdw.SetState(WorkerStateDone)
		return runError
	}

	mdw.wr.Logger().Errorf("Run() error: %v", runError)
	mdw.SetState(WorkerStateError)
	return runError
}

func (mdw *TabletDiffWorker) run(ctx context.Context) error {
	// first state: read what we need to do
	if err := mdw.init(ctx); err != nil {
		return fmt.Errorf("init() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// second phase: synchronize replication
	if err := mdw.syncTabletState(ctx); err != nil {
		return fmt.Errorf("init() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// third phase: diff
	if err := mdw.diff(ctx); err != nil {
		return fmt.Errorf("diff() failed: %v", err)
	}

	return nil
}

// init phase:
// - read the tablet information, make sure it makes sense to diff between them
func (mdw *TabletDiffWorker) init(ctx context.Context) error {
	mdw.SetState(WorkerStateInit)

	var err error
	mdw.destAlias, err = topoproto.ParseTabletAlias(mdw.destTablet)
	if err != nil {
		return fmt.Errorf("cannot parse table alias %s\n%v", mdw.destTablet, err)
	}

	mdw.sourceAlias, err = topoproto.ParseTabletAlias(mdw.sourceTablet)
	if err != nil {
		return fmt.Errorf("cannot parse table alias %s\n%v", mdw.sourceTablet, err)
	}

	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	mdw.destination, err = mdw.wr.TopoServer().GetTablet(shortCtx, mdw.destAlias)
	cancel()
	if err != nil {
		return fmt.Errorf("could not locate tablet: %s\n%v", mdw.destAlias, err)
	}

	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	mdw.source, err = mdw.wr.TopoServer().GetTablet(shortCtx, mdw.sourceAlias)
	cancel()
	if err != nil {
		return fmt.Errorf("could not locate tablet: %s\n%v", mdw.sourceAlias, err)
	}

	if mdw.source.Keyspace != mdw.destination.Keyspace ||
		mdw.source.Shard != mdw.destination.Shard {
		return fmt.Errorf("can only checksum between tablets in the same keyspace and shard")
	}

	return nil
}

func (mdw *TabletDiffWorker) diff(ctx context.Context) error {
	mdw.SetState(WorkerStateDiff)
	tm := tmclient.NewTabletManagerClient()
	defer tm.Close()

	schema, err := tm.GetSchema(ctx, mdw.source.Tablet, []string{}, []string{}, false)
	if err != nil {
		return fmt.Errorf("could not get schema for %v\n%v", mdw.source.Tablet, err)
	}

	numberOfTables := len(schema.TableDefinitions)
	tableChan := make(chan *tabletmanagerdata.TableDefinition, numberOfTables)
	for _, tableDefinition := range schema.TableDefinitions {
		tableChan <- tableDefinition
	}
	close(tableChan)

	diffErrors := make(chan error, numberOfTables)
	wg := &sync.WaitGroup{}

	// Start as many goroutines as we want concurrent diffs happening
	for _, conn := range mdw.connections {
		wg.Add(1)
		go func(conn TableScanner) {
			defer wg.Done()
			for table := range tableChan {
				if !mdw.tableIsExcluded(table.Name) {
					if err = checkDone(ctx); err != nil {
						mdw.wr.Logger().Errorf("cancelled while diffing table `%v`: %v", table.Name, err)
						diffErrors <- err
						return
					}

					err = mdw.diffSingleTable(ctx, table, conn)
					if err != nil {
						mdw.wr.Logger().Errorf("failed to diff table `%v`: %v", table.Name, err)
						diffErrors <- err
					}
				}
			}
		}(conn)
	}

	wg.Wait()
	close(diffErrors)
	var finalErr error

	for e := range diffErrors {
		if e != nil {
			finalErr = errors.New("some validation errors - see log" + e.Error())
		}
	}
	return finalErr
}

func (mdw *TabletDiffWorker) syncTabletState(ctx context.Context) error {
	// 1. First we stop replication on the target, so no transactions sneak by
	tm := tmclient.NewTabletManagerClient()
	defer tm.Close()

	err := tm.StopSlave(ctx, mdw.destination.Tablet)
	if err != nil {
		return fmt.Errorf("could not stop replication on destination %v\n%v", mdw.destination, err)
	}
	mdw.wr.Logger().Infof("replication on the destination stopped")
	wrangler.RecordStartSlaveAction(mdw.cleaner, mdw.destination.Tablet)

	// 2. Next we stop updates to the source so we can create consistent snapshot transactions on the same spot
	connections, pos, err := CreateConsistentTableScanners(ctx, mdw.source, mdw.wr, mdw.cleaner, int(mdw.concurrentTables))
	if err != nil {
		return fmt.Errorf("synchronizeReplication() failed: %v", err)
	}
	mdw.connections = connections
	mdw.executedGtid = pos

	// 3. Resume replication until we reach the source GTID
	for i := 0; i < 20; i++ {
		mdw.wr.Logger().Infof("stopping replica %v at %v", mdw.source.Alias, pos)
		err = tm.StartSlaveUntilAfter(ctx, mdw.destination.Tablet, pos, *remoteActionsTimeout)
		if err != nil {
			return fmt.Errorf("could not stop replication on destination %v\n%v", mdw.destination.Tablet, err)
		}
		newPosition, err := tm.MasterPosition(ctx, mdw.destination.Tablet)
		if err != nil {
			return fmt.Errorf("could not check which transaction replica %v is on: %v", mdw.destination.Tablet, err)
		}

		if pos == newPosition {
			mdw.wr.Logger().Infof("replication on the destination has now reached %s", newPosition)
			return nil
		}

		mdw.wr.Logger().Infof("replica not at the correct position. Retrying")
		time.Sleep(time.Second)
	}

	return fmt.Errorf("failed to get replicas to catch up with source")
}

func (mdw *TabletDiffWorker) diffSingleTable(ctx context.Context, tableDefinition *tabletmanagerdata.TableDefinition, conn TableScanner) error {
	var destinationQueryResultReader *QueryResultReader

	logger := mdw.wr.Logger()
	destinationQueryResultReader, err := TableScan(ctx, logger, mdw.wr.TopoServer(), mdw.destAlias, tableDefinition)
	if err != nil {
		return fmt.Errorf("TableScan(destination) failed: %v", err)
	}
	defer destinationQueryResultReader.Close(ctx)

	sourceQueryResultReader, err := conn.ScanTable(ctx, tableDefinition)
	if err != nil {
		return err
	}
	defer sourceQueryResultReader.Close(ctx)

	logger.Infof("Starting the diff on table %v", tableDefinition.Name)
	differ, err := NewRowDiffer(sourceQueryResultReader, destinationQueryResultReader, tableDefinition)
	if err != nil {
		return fmt.Errorf("could not create row differ %v\n%v", mdw.destination.Tablet, err)
	}

	report, err := differ.Go(logger)

	if err != nil {
		return fmt.Errorf("could not stop replication on destination %v\n%v", mdw.destination.Tablet, err)
	}

	if report.HasDifferences() {
		mdw.wr.Logger().Errorf("%v", err)
		return fmt.Errorf("table %v has differences: %v", tableDefinition.Name, report.String())
	}

	mdw.wr.Logger().Infof("Table %v checks out (%v rows processed, %v qps)", tableDefinition.Name, report.processedRows, report.processingQPS)

	return nil
}

func (mdw *TabletDiffWorker) tableIsExcluded(e string) bool {
	for _, a := range mdw.excludedTables {
		if a == e {
			return true
		}
	}
	return false
}
