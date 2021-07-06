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

package controller

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"

	"vitess.io/vitess/go/vt/topo/memorytopo"

	"vitess.io/vitess/go/vt/vtgr/config"

	"vitess.io/vitess/go/mysql"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/orchestrator/inst"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"

	gomock "github.com/golang/mock/gomock"

	db2 "vitess.io/vitess/go/vt/vtgr/db"
)

const repairGroupSize = 2

func TestRepairShardHasNoGroup(t *testing.T) {
	type data struct {
		alias      string
		mysqlhost  string
		mysqlport  int
		groupName  string
		readOnly   bool
		groupInput []db2.TestGroupState
		ttype      topodatapb.TabletType
	}
	var testcases = []struct {
		name          string
		expectedCalls int
		errorMsg      string
		inputs        []data
	}{
		{"shard without group", 1, "", []data{
			{alias0, testHost, testPort0, "", true, []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, topodatapb.TabletType_REPLICA},
			{alias1, testHost, testPort1, "", true, []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, topodatapb.TabletType_REPLICA},
			{alias2, testHost, testPort2, "", true, []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, topodatapb.TabletType_REPLICA},
		}},
		{"healthy shard", 0, "", []data{
			{alias0, testHost, testPort0, "group", false, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, testHost, testPort1, "group", true, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, testHost, testPort2, "group", true, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{"no active member for group", 0, "", []data{ // this should rebootstrap a group by DiagnoseTypeShardHasInactiveGroup
			{alias0, testHost, testPort0, "group", true, []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, topodatapb.TabletType_REPLICA},
			{alias1, testHost, testPort1, "", false, []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "ERROR", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, testHost, testPort2, "", true, []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{"raise error for unreachable primary", 0, "", []data{ // shoud be ShardHasInactiveGroup
			{alias0, testHost, testPort0, "group", true, []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, topodatapb.TabletType_REPLICA},
			{alias1, testHost, testPort1, "", true, []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "ERROR", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, testHost, testPort2, "", true, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "UNREACHABLE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{"raise error without bootstrap with only one reachable node", 0, "vtgr repair: unsafe to bootstrap group", []data{
			{alias0, "", 0, "group", true, []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, topodatapb.TabletType_REPLICA},
			{alias1, testHost, testPort1, "", true, []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, topodatapb.TabletType_REPLICA},
			{alias2, "", testPort2, "", true, []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, topodatapb.TabletType_REPLICA},
		}},
	}
	tablets := make(map[string]*topo.TabletInfo)
	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ts := memorytopo.NewServer("test_cell")
			defer ts.Close()
			ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{})
			ts.CreateShard(ctx, "ks", "0")
			tmc := NewMockGRTmcClient(ctrl)
			db := db2.NewMockAgent(ctrl)
			inputMap := make(map[int]testGroupInput)
			db.
				EXPECT().
				// RepairShardHasNoGroup is fixed by calling BootstrapGroupLocked
				BootstrapGroupLocked(gomock.Any()).
				DoAndReturn(func(target *inst.InstanceKey) error {
					if target.Hostname == "" || target.Port == 0 {
						return errors.New("invalid mysql instance key")
					}
					input := inputMap[target.Port]
					groupState := input.groupState
					if len(groupState) == 1 && groupState[0].MemberState == "OFFLINE" {
						groupState[0].MemberState = "ONLINE"
						groupState[0].MemberRole = "PRIMARY"
						groupState[0].MemberHost = target.Hostname
						groupState[0].MemberPort = strconv.Itoa(target.Port)
						input.groupState = groupState
					} else {
						for i, s := range groupState {
							if s.MemberHost == target.Hostname {
								s.MemberState = "ONLINE"
								s.MemberRole = "PRIMARY"
								groupState[i] = s
							}
							input.groupState = groupState
						}
					}
					inputMap[target.Port] = input
					return nil
				}).
				Times(tt.expectedCalls)
			for i, input := range tt.inputs {
				tablet := buildTabletInfo(uint32(i), input.mysqlhost, testPort0+i, input.ttype, time.Now())
				testutil.AddTablet(ctx, t, ts, tablet.Tablet, nil)
				tablets[input.alias] = tablet
				inputMap[input.mysqlport] = testGroupInput{
					input.groupName,
					input.readOnly,
					input.groupInput,
					nil,
				}
				db.
					EXPECT().
					FetchGroupView(gomock.Eq(input.alias), gomock.Any()).
					DoAndReturn(func(alias string, target *inst.InstanceKey) (*db2.GroupView, error) {
						if target.Hostname == "" || target.Port == 0 {
							return nil, errors.New("invalid mysql instance key")
						}
						s := inputMap[target.Port]
						view := db2.BuildGroupView(alias, s.groupName, target.Hostname, target.Port, s.readOnly, s.groupState)
						return view, nil
					}).
					AnyTimes()
			}
			tmc.
				EXPECT().
				Ping(gomock.Any(), gomock.Any()).
				Return(nil).
				AnyTimes()
			cfg := &config.VTGRConfig{GroupSize: repairGroupSize, MinNumReplica: 2, BackoffErrorWaitTimeSeconds: 1, BootstrapWaitTimeSeconds: 1}
			shard := NewGRShard("ks", "0", nil, tmc, ts, db, cfg, testPort0)
			shard.UpdateTabletsInShardWithLock(ctx)
			_, err := shard.Repair(ctx, DiagnoseTypeShardHasNoGroup)
			if tt.errorMsg == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tt.errorMsg)
			}
		})
	}
}

func TestRepairShardHasInactiveGroup(t *testing.T) {
	type data struct {
		alias      string
		mysqlhost  string
		mysqlport  int
		groupName  string
		groupInput []db2.TestGroupState
		pingable   bool
		gtid       mysql.GTIDSet
		ttype      topodatapb.TabletType
	}
	sid1 := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	var testcases = []struct {
		name                  string
		errorMsg              string
		expectedCandidatePort int
		inputs                []data
	}{
		{"shard has inactive group", "", testPort0, []data{
			{alias0, testHost, testPort0, "group", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid1, "1-10"), topodatapb.TabletType_REPLICA},
			{alias1, testHost, testPort1, "group", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid1, "1-9"), topodatapb.TabletType_MASTER},
			{alias2, testHost, testPort2, "group", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid1, "1-9"), topodatapb.TabletType_REPLICA},
		}},
		{"unreachable rebootstrap candidate", "vtgr repair: test_cell-0000000000 is unreachable", 0, []data{
			{alias0, testHost, testPort0, "group", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, false, getMysql56GTIDSet(sid1, "1-10"), topodatapb.TabletType_REPLICA},
			{alias1, testHost, testPort1, "group", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid1, "1-9"), topodatapb.TabletType_MASTER},
			{alias2, testHost, testPort2, "group", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid1, "1-9"), topodatapb.TabletType_REPLICA},
		}},
		{"inactive shard with empty gtid", "", testPort0, []data{
			{alias0, testHost, testPort0, "group", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid1, "1-10"), topodatapb.TabletType_REPLICA},
			{alias1, testHost, testPort1, "group", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet("", ""), topodatapb.TabletType_REPLICA},
			{alias2, testHost, testPort2, "group", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet("", ""), topodatapb.TabletType_REPLICA},
		}},
		{"shard has more than one group", "vtgr repair: fail to refreshSQLGroup: group has split brain", 0, []data{ // vtgr raises error
			{alias0, testHost, testPort0, "group1", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid1, "1-9"), topodatapb.TabletType_REPLICA},
			{alias1, testHost, testPort1, "group2", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid1, "1-10"), topodatapb.TabletType_REPLICA},
			{alias2, testHost, testPort2, "group1", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid1, "1-9"), topodatapb.TabletType_REPLICA},
		}},
		{"shard has inconsistent gtids", "vtgr repair: found more than one failover candidates by GTID set for ks/0", 0, []data{ // vtgr raises error
			{alias0, testHost, testPort0, "group", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet("264a8230-67d2-11eb-acdd-0a8d91f24125", "1-9"), topodatapb.TabletType_REPLICA},
			{alias1, testHost, testPort1, "group", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid1, "1-10"), topodatapb.TabletType_REPLICA},
			{alias2, testHost, testPort2, "group", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid1, "1-9"), topodatapb.TabletType_REPLICA},
		}},
		{"error on one unreachable mysql", "invalid mysql instance key", 0, []data{
			{alias0, "", 0, "group", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid1, "1-11"), topodatapb.TabletType_REPLICA},
			{alias1, testHost, testPort1, "group", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid1, "1-10"), topodatapb.TabletType_REPLICA},
			{alias2, testHost, testPort2, "group", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid1, "1-9"), topodatapb.TabletType_REPLICA},
		}},
		{"error on one unreachable tablet", "vtgr repair: test_cell-0000000000 is unreachable", 0, []data{
			{alias0, testHost, testPort0, "group", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, false, getMysql56GTIDSet(sid1, "1-10"), topodatapb.TabletType_REPLICA},
			{alias1, testHost, testPort1, "group", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid1, "1-9"), topodatapb.TabletType_REPLICA},
			{alias2, testHost, testPort2, "group", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid1, "1-9"), topodatapb.TabletType_REPLICA},
		}},
		{"shard has active member", "", 0, []data{ // vtgr sees an active node it should not try to bootstrap
			{alias0, testHost, testPort0, "group", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid1, "1-10"), topodatapb.TabletType_REPLICA},
			{alias1, testHost, testPort1, "group", []db2.TestGroupState{
				{MemberHost: "host_2", MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
			}, true, getMysql56GTIDSet(sid1, "1-9"), topodatapb.TabletType_REPLICA},
			{alias2, testHost, testPort2, "group", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid1, "1-9"), topodatapb.TabletType_REPLICA},
		}},
		{"shard has active member but more than one group", "vtgr repair: fail to refreshSQLGroup: group has split brain", 0, []data{ // split brain should overweight active member diagnose
			{alias0, testHost, testPort0, "group1", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid1, "1-10"), topodatapb.TabletType_REPLICA},
			{alias1, testHost, testPort1, "group1", []db2.TestGroupState{
				{MemberHost: "host_2", MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
			}, true, getMysql56GTIDSet(sid1, "1-9"), topodatapb.TabletType_REPLICA},
			{alias2, testHost, testPort2, "group2", []db2.TestGroupState{
				{MemberHost: "", MemberPort: "NULL", MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid1, "1-9"), topodatapb.TabletType_REPLICA},
		}},
	}
	tablets := make(map[string]*topo.TabletInfo)
	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ctx := context.Background()
			ts := memorytopo.NewServer("test_cell")
			defer ts.Close()
			ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{})
			ts.CreateShard(ctx, "ks", "0")
			tmc := NewMockGRTmcClient(ctrl)
			db := db2.NewMockAgent(ctrl)
			expectedCalls := 0
			if tt.expectedCandidatePort != 0 {
				expectedCalls = 1
			}
			inputMap := make(map[int]testGroupInput)
			pingable := make(map[string]bool)
			var lock sync.Mutex
			db.
				EXPECT().
				// RepairShardHasNoGroup is fixed by calling BootstrapGroupLocked
				BootstrapGroupLocked(&inst.InstanceKey{Hostname: testHost, Port: tt.expectedCandidatePort}).
				DoAndReturn(func(target *inst.InstanceKey) error {
					if target.Hostname == "" || target.Port == 0 {
						return errors.New("invalid mysql instance key")
					}
					input := inputMap[target.Port]
					groupState := input.groupState
					if len(groupState) == 1 && groupState[0].MemberState == "OFFLINE" {
						groupState[0].MemberState = "ONLINE"
						groupState[0].MemberRole = "PRIMARY"
						groupState[0].MemberHost = target.Hostname
						groupState[0].MemberPort = strconv.Itoa(target.Port)
						input.groupState = groupState
					} else {
						for i, s := range groupState {
							if s.MemberHost == target.Hostname {
								s.MemberState = "ONLINE"
								s.MemberRole = "PRIMARY"
								groupState[i] = s
							}
							input.groupState = groupState
						}
					}
					inputMap[target.Port] = input
					return nil
				}).
				Times(expectedCalls)
			for i, input := range tt.inputs {
				tablet := buildTabletInfo(uint32(i), input.mysqlhost, input.mysqlport, input.ttype, time.Now())
				testutil.AddTablet(ctx, t, ts, tablet.Tablet, nil)
				tablets[input.alias] = tablet
				inputMap[input.mysqlport] = testGroupInput{
					input.groupName,
					false,
					input.groupInput,
					input.gtid,
				}
				pingable[tablet.Alias.String()] = input.pingable
				db.
					EXPECT().
					FetchGroupView(gomock.Eq(input.alias), gomock.Any()).
					DoAndReturn(func(alias string, target *inst.InstanceKey) (*db2.GroupView, error) {
						if target.Hostname == "" || target.Port == 0 {
							return nil, errors.New("invalid mysql instance key")
						}
						s := inputMap[target.Port]
						view := db2.BuildGroupView(alias, s.groupName, target.Hostname, target.Port, s.readOnly, s.groupState)
						return view, nil
					}).
					AnyTimes()
				db.
					EXPECT().
					FetchApplierGTIDSet(gomock.Any()).
					DoAndReturn(func(target *inst.InstanceKey) (mysql.GTIDSet, error) {
						if target.Hostname == "" || target.Port == 0 {
							return nil, errors.New("invalid mysql instance key")
						}
						return inputMap[target.Port].gtid, nil
					}).
					AnyTimes()
				db.
					EXPECT().
					StopGroupLocked(gomock.Any()).
					DoAndReturn(func(target *inst.InstanceKey) error {
						if target.Hostname == "" || target.Port == 0 {
							return errors.New("invalid mysql instance key")
						}
						lock.Lock()
						view := inputMap[target.Port]
						view.groupState = []db2.TestGroupState{
							{MemberHost: testHost, MemberPort: strconv.Itoa(target.Port), MemberState: "OFFLINE", MemberRole: ""},
						}
						inputMap[target.Port] = view
						lock.Unlock()
						return nil
					}).
					AnyTimes()
				tmc.
					EXPECT().
					Ping(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, t *topodatapb.Tablet) error {
						if !pingable[t.Alias.String()] {
							return errors.New("unreachable")
						}
						return nil
					}).
					AnyTimes()
			}
			cfg := &config.VTGRConfig{GroupSize: repairGroupSize, MinNumReplica: 2, BackoffErrorWaitTimeSeconds: 1, BootstrapWaitTimeSeconds: 1}
			shard := NewGRShard("ks", "0", nil, tmc, ts, db, cfg, testPort0)
			_, err := shard.Repair(ctx, DiagnoseTypeShardHasInactiveGroup)
			if tt.errorMsg == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err, tt.errorMsg)
				assert.True(t, strings.Contains(err.Error(), tt.errorMsg), err.Error())
			}
		})
	}
}

func TestRepairWrongPrimaryTablet(t *testing.T) {
	type data struct {
		alias      string
		mysqlport  int
		groupName  string
		groupInput []db2.TestGroupState
		ttype      topodatapb.TabletType
	}

	var testcases = []struct {
		name                  string
		errorMsg              string
		expectedCandidatePort int
		inputs                []data
	}{
		{"fix no primary tablet in shard", "", testPort0, []data{
			{alias0, testPort0, "group", []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias1, testPort1, "group", []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, testPort2, "group", []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{"fix wrong primary tablet", "", testPort0, []data{
			{alias0, testPort0, "group", []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias1, testPort1, "group", []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias2, testPort2, "group", []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{"fix shard if there is an unreachable secondary", "", testPort0, []data{
			{alias0, testPort0, "group", []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "UNREACHABLE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias1, testPort1, "group", []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "UNREACHABLE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias2, testPort2, "group", []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "UNREACHABLE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{"diagnose as ShardHasInactiveGroup if quorum number of not online", "", 0, []data{
			{alias0, testPort0, "group", []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "UNREACHABLE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "UNREACHABLE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias1, testPort1, "group", []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "UNREACHABLE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias2, testPort2, "group", []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "UNREACHABLE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{"tolerate failed nodes", "", testPort0, []data{
			{alias0, testPort0, "group", []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "UNREACHABLE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ERROR", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias1, 0, "group", []db2.TestGroupState{}, topodatapb.TabletType_MASTER},
			{alias2, 0, "group", []db2.TestGroupState{}, topodatapb.TabletType_REPLICA},
		}},
		{"raise error if all nodes failed", "", 0, []data{ // diagnose as DiagnoseTypeShardNetworkPartition
			{alias0, 0, "group", []db2.TestGroupState{}, topodatapb.TabletType_REPLICA},
			{alias1, 0, "group", []db2.TestGroupState{}, topodatapb.TabletType_MASTER},
			{alias2, 0, "group", []db2.TestGroupState{}, topodatapb.TabletType_REPLICA},
		}},
	}
	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ctx := context.Background()
			ts := memorytopo.NewServer("test_cell")
			defer ts.Close()
			ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{})
			ts.CreateShard(ctx, "ks", "0")
			tmc := NewMockGRTmcClient(ctrl)
			db := db2.NewMockAgent(ctrl)
			tablets := make(map[string]*topo.TabletInfo)
			tmc.
				EXPECT().
				Ping(gomock.Any(), gomock.Any()).
				Return(nil).
				AnyTimes()
			expectedCalls := 0
			if tt.expectedCandidatePort != 0 {
				expectedCalls = 1
			}
			var candidate *topo.TabletInfo
			inputMap := make(map[string]testGroupInput)
			for i, input := range tt.inputs {
				tablet := buildTabletInfo(uint32(i), testHost, input.mysqlport, input.ttype, time.Now())
				testutil.AddTablet(ctx, t, ts, tablet.Tablet, nil)
				tablets[input.alias] = tablet
				inputMap[input.alias] = testGroupInput{
					input.groupName,
					false,
					input.groupInput,
					nil,
				}
				if expectedCalls > 0 && input.mysqlport == tt.expectedCandidatePort {
					candidate = tablet
				}
				db.
					EXPECT().
					FetchGroupView(gomock.Eq(input.alias), gomock.Eq(&inst.InstanceKey{Hostname: testHost, Port: input.mysqlport})).
					DoAndReturn(func(alias string, target *inst.InstanceKey) (*db2.GroupView, error) {
						if target.Hostname == "" || target.Port == 0 {
							return nil, errors.New("invalid mysql instance key")
						}
						s := inputMap[alias]
						view := db2.BuildGroupView(alias, s.groupName, target.Hostname, target.Port, s.readOnly, s.groupState)
						return view, nil
					}).
					AnyTimes()
			}
			if candidate != nil {
				tmc.
					EXPECT().
					ChangeType(gomock.Any(), gomock.Any(), topodatapb.TabletType_MASTER).
					Return(nil).
					Times(expectedCalls)
			}
			cfg := &config.VTGRConfig{GroupSize: repairGroupSize, MinNumReplica: 2, BackoffErrorWaitTimeSeconds: 1, BootstrapWaitTimeSeconds: 1}
			shard := NewGRShard("ks", "0", nil, tmc, ts, db, cfg, testPort0)
			_, err := shard.Repair(ctx, DiagnoseTypeWrongPrimaryTablet)
			if tt.errorMsg == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), tt.errorMsg), err.Error())
			}
		})
	}
}

func TestRepairUnconnectedReplica(t *testing.T) {
	type data struct {
		alias      string
		port       int
		groupName  string
		readOnly   bool
		groupInput []db2.TestGroupState
		ttype      topodatapb.TabletType
	}
	var testcases = []struct {
		name                  string
		errorMsg              string
		expectedCandidatePort int
		inputs                []data
	}{
		{"fix unconnected replica tablet", "", testPort2, []data{
			{alias0, testPort0, "group", false, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, testPort1, "group", true, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, testPort2, "", true, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "OFFLINE", MemberRole: ""},
			}, topodatapb.TabletType_REPLICA},
		}},
		{"do nothing if shard has wrong primary tablet", "", 0, []data{ // this should be diagnosed as DiagnoseTypeWrongPrimaryTablet instead
			{alias0, testPort0, "group", true, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "PRIMARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, testPort1, "group", false, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "PRIMARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, testPort2, "", true, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "OFFLINE", MemberRole: ""},
			}, topodatapb.TabletType_REPLICA},
		}},
		{"fix replica in ERROR state", "", testPort2, []data{
			{alias0, testPort0, "group", false, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, testPort1, "group", true, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, testPort2, "group", true, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ERROR", MemberRole: ""},
			}, topodatapb.TabletType_REPLICA},
		}},
		{"fix replica with two nodes in ERROR state", "", 0, []data{ // InsufficientGroupSize
			{alias0, testPort0, "group", false, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, testPort1, "group", true, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ERROR", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, testPort2, "group", true, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ERROR", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
	}
	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			rand.Seed(1)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ctx := context.Background()
			ts := memorytopo.NewServer("test_cell")
			defer ts.Close()
			ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{})
			ts.CreateShard(ctx, "ks", "0")
			tmc := NewMockGRTmcClient(ctrl)
			db := db2.NewMockAgent(ctrl)
			tablets := make(map[string]*topo.TabletInfo)
			tmc.
				EXPECT().
				Ping(gomock.Any(), gomock.Any()).
				Return(nil).
				AnyTimes()
			if tt.expectedCandidatePort != 0 {
				db.
					EXPECT().
					StopGroupLocked(gomock.Eq(&inst.InstanceKey{Hostname: testHost, Port: tt.expectedCandidatePort})).
					Return(nil).
					AnyTimes()
				db.
					EXPECT().
					JoinGroupLocked(gomock.Eq(&inst.InstanceKey{Hostname: testHost, Port: tt.expectedCandidatePort}), gomock.Any()).
					Return(nil).
					Times(1)
			}
			inputMap := make(map[string]testGroupInput)
			for i, input := range tt.inputs {
				tablet := buildTabletInfo(uint32(i), testHost, input.port, input.ttype, time.Now())
				testutil.AddTablet(ctx, t, ts, tablet.Tablet, nil)
				tablets[input.alias] = tablet
				inputMap[input.alias] = testGroupInput{
					input.groupName,
					input.readOnly,
					input.groupInput,
					nil,
				}
				db.
					EXPECT().
					FetchGroupView(gomock.Eq(input.alias), gomock.Eq(&inst.InstanceKey{Hostname: testHost, Port: input.port})).
					DoAndReturn(func(alias string, target *inst.InstanceKey) (*db2.GroupView, error) {
						if target.Hostname == "" || target.Port == 0 {
							return nil, errors.New("invalid mysql instance key")
						}
						s := inputMap[alias]
						view := db2.BuildGroupView(alias, s.groupName, target.Hostname, target.Port, s.readOnly, s.groupState)
						return view, nil
					}).
					AnyTimes()
			}
			cfg := &config.VTGRConfig{GroupSize: repairGroupSize, MinNumReplica: 2, BackoffErrorWaitTimeSeconds: 1, BootstrapWaitTimeSeconds: 1}
			shard := NewGRShard("ks", "0", nil, tmc, ts, db, cfg, testPort0)
			_, err := shard.Repair(ctx, DiagnoseTypeUnconnectedReplica)
			if tt.errorMsg == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), tt.errorMsg), err.Error())
			}
		})
	}
}

func TestRepairUnreachablePrimary(t *testing.T) {
	type data struct {
		port     int
		pingalbe bool
		gtid     mysql.GTIDSet
		ttype    topodatapb.TabletType
	}
	sid := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	var testcases = []struct {
		name                  string
		errorMsg              string
		expectedCandidatePort int
		inputs                []data
	}{
		{"primary is unreachable", "", testPort1, []data{
			{testPort0, false, getMysql56GTIDSet(sid, "1-11"), topodatapb.TabletType_MASTER},
			{testPort1, true, getMysql56GTIDSet(sid, "1-10"), topodatapb.TabletType_REPLICA},
			{testPort2, true, getMysql56GTIDSet(sid, "1-9"), topodatapb.TabletType_REPLICA},
		}},
		{"failover to reachable node when primary is unreachable", "", testPort2, []data{
			{testPort0, false, getMysql56GTIDSet(sid, "1-11"), topodatapb.TabletType_MASTER},
			{testPort1, false, getMysql56GTIDSet(sid, "1-10"), topodatapb.TabletType_REPLICA},
			{testPort2, true, getMysql56GTIDSet(sid, "1-9"), topodatapb.TabletType_REPLICA},
		}},
		{"do nothing if replica is unreachable", "", 0, []data{
			{testPort0, true, getMysql56GTIDSet(sid, "1-10"), topodatapb.TabletType_MASTER},
			{testPort1, false, getMysql56GTIDSet(sid, "1-10"), topodatapb.TabletType_REPLICA},
			{testPort2, false, getMysql56GTIDSet(sid, "1-9"), topodatapb.TabletType_REPLICA},
		}},
		{"raise error if gtid divergence", "vtgr repair: found more than one failover candidates by GTID set for ks/0", 0, []data{
			{testPort0, false, getMysql56GTIDSet(sid, "1-10"), topodatapb.TabletType_MASTER},
			{testPort1, true, getMysql56GTIDSet("264a8230-67d2-11eb-acdd-0a8d91f24125", "1-10"), topodatapb.TabletType_REPLICA},
			{testPort2, true, getMysql56GTIDSet(sid, "1-9"), topodatapb.TabletType_REPLICA},
		}},
	}
	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ctx := context.Background()
			ts := memorytopo.NewServer("test_cell")
			defer ts.Close()
			ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{})
			ts.CreateShard(ctx, "ks", "0")
			tmc := NewMockGRTmcClient(ctrl)
			db := db2.NewMockAgent(ctrl)
			db.
				EXPECT().
				FetchGroupView(gomock.Any(), gomock.Any()).
				DoAndReturn(func(alias string, target *inst.InstanceKey) (*db2.GroupView, error) {
					return db2.BuildGroupView(alias, "group", target.Hostname, target.Port, false, []db2.TestGroupState{
						{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
						{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
						{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
					}), nil
				}).
				AnyTimes()
			expectedCalls := 0
			if tt.expectedCandidatePort != 0 {
				expectedCalls = 1
			}
			db.
				EXPECT().
				Failover(&inst.InstanceKey{Hostname: testHost, Port: tt.expectedCandidatePort}).
				Return(nil).
				Times(expectedCalls)
			tmc.
				EXPECT().
				ChangeType(gomock.Any(), gomock.Any(), topodatapb.TabletType_MASTER).
				Return(nil).
				Times(expectedCalls)
			status := make(map[int32]struct {
				pingalbe bool
				gtid     mysql.GTIDSet
			})
			for i, input := range tt.inputs {
				tablet := buildTabletInfo(uint32(i), testHost, input.port, input.ttype, time.Now())
				testutil.AddTablet(ctx, t, ts, tablet.Tablet, nil)
				status[tablet.MysqlPort] = struct {
					pingalbe bool
					gtid     mysql.GTIDSet
				}{
					input.pingalbe,
					input.gtid,
				}
				db.
					EXPECT().
					FetchApplierGTIDSet(gomock.Eq(&inst.InstanceKey{Hostname: testHost, Port: input.port})).
					DoAndReturn(func(target *inst.InstanceKey) (mysql.GTIDSet, error) {
						if target.Hostname == "" || target.Port == 0 {
							return nil, errors.New("invalid mysql instance key")
						}
						return status[int32(target.Port)].gtid, nil
					}).
					AnyTimes()
				tmc.
					EXPECT().
					Ping(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, t *topodatapb.Tablet) error {
						if !status[t.MysqlPort].pingalbe {
							return errors.New("unreachable")
						}
						return nil
					}).
					AnyTimes()
			}
			cfg := &config.VTGRConfig{GroupSize: repairGroupSize, MinNumReplica: 2, BackoffErrorWaitTimeSeconds: 1, BootstrapWaitTimeSeconds: 1}
			shard := NewGRShard("ks", "0", nil, tmc, ts, db, cfg, testPort0)
			_, err := shard.Repair(ctx, DiagnoseTypeUnreachablePrimary)
			if tt.errorMsg == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err, tt.errorMsg)
				assert.True(t, strings.Contains(err.Error(), tt.errorMsg))
			}
		})
	}
}

func TestRepairInsufficientGroupSize(t *testing.T) {
	type data struct {
		alias      string
		readOnly   bool
		groupInput []db2.TestGroupState
		ttype      topodatapb.TabletType
	}
	var testcases = []struct {
		name                  string
		errorMsg              string
		expectedCandidatePort int
		inputs                []data
	}{
		{"fix insufficient group size", "", testPort0, []data{
			{alias0, false, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, true, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, true, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
	}
	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ctx := context.Background()
			ts := memorytopo.NewServer("test_cell")
			defer ts.Close()
			ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{})
			ts.CreateShard(ctx, "ks", "0")
			tmc := NewMockGRTmcClient(ctrl)
			db := db2.NewMockAgent(ctrl)
			tablets := make(map[string]*topo.TabletInfo)
			tmc.
				EXPECT().
				Ping(gomock.Any(), gomock.Any()).
				Return(nil).
				AnyTimes()
			if tt.expectedCandidatePort != 0 {
				db.
					EXPECT().
					SetSuperReadOnly(gomock.Eq(&inst.InstanceKey{Hostname: testHost, Port: tt.expectedCandidatePort}), true).
					Return(nil).
					Times(1)
			}
			inputMap := make(map[string]testGroupInput)
			for i, input := range tt.inputs {
				tablet := buildTabletInfo(uint32(i), testHost, testPort0+i, input.ttype, time.Now())
				testutil.AddTablet(ctx, t, ts, tablet.Tablet, nil)
				tablets[input.alias] = tablet
				inputMap[input.alias] = testGroupInput{
					"group",
					input.readOnly,
					input.groupInput,
					nil,
				}
				db.
					EXPECT().
					FetchGroupView(gomock.Any(), gomock.Any()).
					DoAndReturn(func(alias string, target *inst.InstanceKey) (*db2.GroupView, error) {
						if target.Hostname == "" || target.Port == 0 {
							return nil, errors.New("invalid mysql instance key")
						}
						s := inputMap[alias]
						view := db2.BuildGroupView(alias, s.groupName, target.Hostname, target.Port, s.readOnly, s.groupState)
						return view, nil
					}).
					AnyTimes()
			}
			cfg := &config.VTGRConfig{GroupSize: repairGroupSize, MinNumReplica: 2, BackoffErrorWaitTimeSeconds: 1, BootstrapWaitTimeSeconds: 1}
			shard := NewGRShard("ks", "0", nil, tmc, ts, db, cfg, testPort0)
			_, err := shard.Repair(ctx, DiagnoseTypeInsufficientGroupSize)
			if tt.errorMsg == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), tt.errorMsg), err.Error())
			}
		})
	}
}

func TestRepairReadOnlyShard(t *testing.T) {
	type data struct {
		alias      string
		port       int
		readOnly   bool
		groupInput []db2.TestGroupState
		ttype      topodatapb.TabletType
	}
	var testcases = []struct {
		name                  string
		errorMsg              string
		expectedCandidatePort int
		inputs                []data
	}{
		{"fix readonly shard", "", testPort0, []data{
			{alias0, testPort0, true, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, testPort1, true, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, testPort2, true, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{"do nothing if primary is not read only", "", 0, []data{
			{alias0, testPort0, false, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, testPort1, true, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, testPort2, true, []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
	}
	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ctx := context.Background()
			ts := memorytopo.NewServer("test_cell")
			defer ts.Close()
			ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{})
			ts.CreateShard(ctx, "ks", "0")
			tmc := NewMockGRTmcClient(ctrl)
			db := db2.NewMockAgent(ctrl)
			tablets := make(map[string]*topo.TabletInfo)
			tmc.
				EXPECT().
				Ping(gomock.Any(), gomock.Any()).
				Return(nil).
				AnyTimes()
			if tt.expectedCandidatePort != 0 {
				db.
					EXPECT().
					SetSuperReadOnly(gomock.Eq(&inst.InstanceKey{Hostname: testHost, Port: tt.expectedCandidatePort}), false).
					Return(nil).
					Times(1)
			}
			inputMap := make(map[string]testGroupInput)
			for i, input := range tt.inputs {
				tablet := buildTabletInfo(uint32(i), testHost, input.port, input.ttype, time.Now())
				testutil.AddTablet(ctx, t, ts, tablet.Tablet, nil)
				tablets[input.alias] = tablet
				inputMap[input.alias] = testGroupInput{
					"group",
					input.readOnly,
					input.groupInput,
					nil,
				}
				db.
					EXPECT().
					FetchGroupView(gomock.Eq(input.alias), gomock.Any()).
					DoAndReturn(func(alias string, target *inst.InstanceKey) (*db2.GroupView, error) {
						if target.Hostname == "" || target.Port == 0 {
							return nil, errors.New("invalid mysql instance key")
						}
						s := inputMap[alias]
						view := db2.BuildGroupView(alias, s.groupName, target.Hostname, target.Port, s.readOnly, s.groupState)
						return view, nil
					}).
					AnyTimes()
			}
			cfg := &config.VTGRConfig{GroupSize: repairGroupSize, MinNumReplica: 2, BackoffErrorWaitTimeSeconds: 1, BootstrapWaitTimeSeconds: 1}
			shard := NewGRShard("ks", "0", nil, tmc, ts, db, cfg, testPort0)
			_, err := shard.Repair(ctx, DiagnoseTypeReadOnlyShard)
			if tt.errorMsg == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), tt.errorMsg), err.Error())
			}
		})
	}
}

func TestRepairBackoffError(t *testing.T) {
	type data struct {
		alias      string
		mysqlhost  string
		mysqlport  int
		groupName  string
		groupInput []db2.TestGroupState
		pingable   bool
		gtid       mysql.GTIDSet
		ttype      topodatapb.TabletType
	}
	sid := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	var testcases = []struct {
		name                  string
		errorMsg              string
		expectedCandidatePort int
		diagnose              DiagnoseType
		inputs                []data
	}{
		{"shard has network partition", "", testPort0, DiagnoseTypeBackoffError, []data{
			{alias0, testHost, testPort0, "group", []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "UNREACHABLE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "UNREACHABLE", MemberRole: "SECONDARY"},
			}, true, getMysql56GTIDSet(sid, "1-10"), topodatapb.TabletType_REPLICA},
			{alias1, testHost, testPort1, "group", []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid, "1-9"), topodatapb.TabletType_REPLICA},
			{alias2, testHost, testPort2, "group", []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid, "1-9"), topodatapb.TabletType_REPLICA},
		}},
		{"shard bootstrap in progress", "", testPort0, DiagnoseTypeBootstrapBackoff, []data{
			{alias0, testHost, testPort0, "group", []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, true, getMysql56GTIDSet(sid, "1-10"), topodatapb.TabletType_REPLICA},
			{alias1, testHost, testPort1, "group", []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid, "1-9"), topodatapb.TabletType_REPLICA},
			{alias2, testHost, testPort2, "group", []db2.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "OFFLINE", MemberRole: ""},
			}, true, getMysql56GTIDSet(sid, "1-9"), topodatapb.TabletType_REPLICA},
		}},
	}
	tablets := make(map[string]*topo.TabletInfo)
	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ctx := context.Background()
			ts := memorytopo.NewServer("test_cell")
			defer ts.Close()
			ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{})
			ts.CreateShard(ctx, "ks", "0")
			tmc := NewMockGRTmcClient(ctrl)
			db := db2.NewMockAgent(ctrl)
			expectedCalls := 0
			if tt.expectedCandidatePort != 0 {
				expectedCalls = 1
			}
			inputMap := make(map[int]testGroupInput)
			pingable := make(map[string]bool)
			var lock sync.Mutex
			db.
				EXPECT().
				BootstrapGroupLocked(&inst.InstanceKey{Hostname: testHost, Port: tt.expectedCandidatePort}).
				DoAndReturn(func(target *inst.InstanceKey) error {
					if target.Hostname == "" || target.Port == 0 {
						return errors.New("invalid mysql instance key")
					}
					input := inputMap[target.Port]
					groupState := input.groupState
					if len(groupState) == 1 && groupState[0].MemberState == "OFFLINE" {
						groupState[0].MemberState = "ONLINE"
						groupState[0].MemberRole = "PRIMARY"
						groupState[0].MemberHost = target.Hostname
						groupState[0].MemberPort = strconv.Itoa(target.Port)
						input.groupState = groupState
					} else {
						for i, s := range groupState {
							if s.MemberHost == target.Hostname {
								s.MemberState = "ONLINE"
								s.MemberRole = "PRIMARY"
								groupState[i] = s
							}
							input.groupState = groupState
						}
					}
					inputMap[target.Port] = input
					return nil
				}).
				Times(expectedCalls)
			for i, input := range tt.inputs {
				tablet := buildTabletInfo(uint32(i), input.mysqlhost, input.mysqlport, input.ttype, time.Now())
				testutil.AddTablet(ctx, t, ts, tablet.Tablet, nil)
				tablets[input.alias] = tablet
				inputMap[input.mysqlport] = testGroupInput{
					input.groupName,
					false,
					input.groupInput,
					input.gtid,
				}
				pingable[input.alias] = input.pingable
				db.
					EXPECT().
					FetchGroupView(gomock.Eq(input.alias), gomock.Any()).
					DoAndReturn(func(alias string, target *inst.InstanceKey) (*db2.GroupView, error) {
						if target.Hostname == "" || target.Port == 0 {
							return nil, errors.New("invalid mysql instance key")
						}
						s := inputMap[target.Port]
						view := db2.BuildGroupView(alias, s.groupName, target.Hostname, target.Port, s.readOnly, s.groupState)
						return view, nil
					}).
					AnyTimes()
				db.
					EXPECT().
					FetchApplierGTIDSet(gomock.Any()).
					DoAndReturn(func(target *inst.InstanceKey) (mysql.GTIDSet, error) {
						if target.Hostname == "" || target.Port == 0 {
							return nil, errors.New("invalid mysql instance key")
						}
						return inputMap[target.Port].gtid, nil
					}).
					AnyTimes()
				db.
					EXPECT().
					StopGroupLocked(gomock.Any()).
					DoAndReturn(func(target *inst.InstanceKey) error {
						lock.Lock()
						view := inputMap[target.Port]
						view.groupState = []db2.TestGroupState{
							{MemberHost: testHost, MemberPort: strconv.Itoa(target.Port), MemberState: "OFFLINE", MemberRole: ""},
						}
						inputMap[target.Port] = view
						lock.Unlock()
						return nil
					}).
					AnyTimes()
				tmc.
					EXPECT().
					Ping(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, t *topodatapb.Tablet) error {
						if !pingable[input.alias] {
							return errors.New("unreachable")
						}
						return nil
					}).
					AnyTimes()
			}
			cfg := &config.VTGRConfig{GroupSize: repairGroupSize, MinNumReplica: 2, BackoffErrorWaitTimeSeconds: 1, BootstrapWaitTimeSeconds: 1}
			shard := NewGRShard("ks", "0", nil, tmc, ts, db, cfg, testPort0)
			shard.lastDiagnoseResult = tt.diagnose
			_, err := shard.Repair(ctx, tt.diagnose)
			if tt.errorMsg == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err, tt.errorMsg)
				assert.True(t, strings.Contains(err.Error(), tt.errorMsg), err.Error())
			}
		})
	}
}

func getMysql56GTIDSet(sid, interval string) mysql.GTIDSet {
	input := fmt.Sprintf("%s:%s", sid, interval)
	pos, _ := mysql.ParsePosition(mysql.Mysql56FlavorID, input)
	return pos.GTIDSet
}
