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

package vschema

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"testing"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams

	hostname     = "localhost"
	keyspaceName = "ks"
	cell         = "zone1"
	sqlSchema    = `
		create table vt_user (
			id bigint,
			name varchar(64),
			primary key (id)
		) Engine=InnoDB;
			
		create table main (
			id bigint,
			val varchar(128),
			primary key(id)
		) Engine=InnoDB;

		create table vstream_test(
			id bigint,
			val bigint,
			primary key(id)
		) Engine=InnoDB;
`

	vschema = &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
			},
			"t1_id2_vdx": {
				Type: "consistent_lookup_unique",
				Params: map[string]string{
					"table": "t1_id2_idx",
					"from":  "id2",
					"to":    "keyspace_id",
				},
				Owner: "t1",
			},
			"t2_id4_idx": {
				Type: "lookup_hash",
				Params: map[string]string{
					"table":      "t2_id4_idx",
					"from":       "id4",
					"to":         "id3",
					"autocommit": "true",
				},
				Owner: "t2",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id1",
					Name:   "hash",
				}, {
					Column: "id2",
					Name:   "t1_id2_vdx",
				}},
			},
			"t1_id2_idx": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id2",
					Name:   "hash",
				}},
			},
			"t2": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id3",
					Name:   "hash",
				}, {
					Column: "id4",
					Name:   "t2_id4_idx",
				}},
			},
			"t2_id4_idx": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id4",
					Name:   "hash",
				}},
			},
			"vstream_test": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id",
					Name:   "hash",
				}},
			},
			"aggr_test": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id",
					Name:   "hash",
				}},
				Columns: []*vschemapb.Column{{
					Name: "val1",
					Type: sqltypes.VarChar,
				}},
			},
			"t1_last_insert_id": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id1",
					Name:   "hash",
				}},
				Columns: []*vschemapb.Column{{
					Name: "id1",
					Type: sqltypes.Int64,
				}},
			},
			"t1_row_count": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id",
					Name:   "hash",
				}},
			},
		},
	}
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// List of users authorized to execute vschema ddl operations
		clusterInstance.VtGateExtraArgs = []string{"-vschema_ddl_authorized_users=%"}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false); err != nil {
			return 1, err
		}

		// Start sharded keyspace
		keyspace = cluster.Keyspace{
			Name:      "ks",
			SchemaSQL: sqlSchema,
			VSchema:   vschema.String(),
		}
		if err := clusterInstance.StartKeyspace(keyspace, []string{"-80", "80-"}, 1, false); err != nil {
			return 1, err
		}

		// Start vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1, err
		}
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}

}

func TestVStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer cluster.PanicHandler(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	grpcAddress := fmt.Sprintf("%s:%d", clusterInstance.Hostname, clusterInstance.VtgateProcess.GrpcPort)
	gconn, err := vtgateconn.Dial(ctx, grpcAddress)
	require.NoError(t, err)

	myParams := mysql.ConnParams{
		Uname:      "vt_dba",
		UnixSocket: path.Join(clusterInstance.Keyspaces[0].Shards[0].MasterTablet().VttabletProcess.Directory, "/mysql.sock"),
	}
	mconn, err := mysql.Connect(ctx, &myParams)
	require.NoError(t, err)

	defer func() {
		gconn.Close()
		conn.Close()
		mconn.Close()
	}()

	mpos, err := mconn.MasterPosition()
	require.NoError(t, err)
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: "ks",
			Shard:    "-80",
			Gtid:     fmt.Sprintf("%s/%s", mpos.GTIDSet.Flavor(), mpos),
		}},
	}
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*/",
		}},
	}
	reader, err := gconn.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, filter)
	require.NoError(t, err)

	_, err = conn.ExecuteFetch("insert into vstream_test(id,val) values(1,1), (4,4)", 1, false)
	require.NoError(t, err)
	// We expect two events because the insert goes to two shards (-80 and 80-),
	// and both of them are in the same mysql server.
	// The row that goes to 80- will have events.
	// The other will be an empty transaction.
	// In a real world scenario where every mysql instance hosts only one
	// keyspace/shard, we should expect only a single event.
	// The events could come in any order as the scatter insert runs in parallel.
	emptyEventSkipped := false
	for i := 0; i < 2; i++ {
		events, err := reader.Recv()
		require.NoError(t, err)
		fmt.Printf("events: %v\n", events)
		// An empty transaction has three events: begin, gtid and commit.
		if len(events) == 3 && !emptyEventSkipped {
			emptyEventSkipped = true
			continue
		}
		if len(events) != 5 {
			t.Errorf("Unexpected event length: %v", events)
			continue
		}
		wantFields := &binlogdatapb.FieldEvent{
			TableName: "ks.vstream_test",
			Fields: []*querypb.Field{{
				Name: "id",
				Type: querypb.Type_INT64,
			}, {
				Name: "val",
				Type: querypb.Type_INT64,
			}},
		}

		gotFields := events[1].FieldEvent
		filteredFields := &binlogdatapb.FieldEvent{
			TableName: gotFields.TableName,
			Fields:    []*querypb.Field{},
		}
		for _, field := range gotFields.Fields {
			filteredFields.Fields = append(filteredFields.Fields, &querypb.Field{
				Name: field.Name,
				Type: field.Type,
			})
		}
		if !proto.Equal(filteredFields, wantFields) {
			t.Errorf("FieldEvent:\n%v, want\n%v", gotFields, wantFields)
		}
		wantRows := &binlogdatapb.RowEvent{
			TableName: "ks.vstream_test",
			RowChanges: []*binlogdatapb.RowChange{{
				After: &query.Row{
					Lengths: []int64{1, 1},
					Values:  []byte("11"),
				},
			}},
		}
		gotRows := events[2].RowEvent
		if !proto.Equal(gotRows, wantRows) {
			t.Errorf("RowEvent:\n%v, want\n%v", gotRows, wantRows)
		}
	}
	cancel()
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	if err != nil {
		t.Fatal(err)
	}
	return qr
}
