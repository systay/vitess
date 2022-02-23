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

package insert

import (
	"flag"
	"os"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	sKs             = "sks"
	uKs             = "uks"
	Cell            = "test"
	sSchemaSQL      = `create table s_tbl(
	id bigint,
	num bigint,
	primary key(id)
) Engine=InnoDB;

create table num_vdx_tbl(
	num bigint,
	keyspace_id varbinary(20),
	primary key(num)
) Engine=InnoDB;

create table user_tbl(
	id bigint,
    region_id bigint,
	name varchar(50),
	primary key(id)
) Engine=InnoDB;
`

	sVSchema = `
{
  "sharded": true,
  "vindexes": {
    "hash": {
      "type": "hash"
    },
    "num_vdx": {
      "type": "consistent_lookup_unique",
      "params": {
        "table": "num_vdx_tbl",
        "from": "num",
        "to": "keyspace_id"
      },
      "owner": "s_tbl"
    }
  },
  "tables": {
    "s_tbl": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "hash"
        },
        {
          "column": "num",
          "name": "num_vdx"
        }
      ]
    },
    "num_vdx_tbl": {
      "column_vindexes": [
        {
          "column": "num",
          "name": "hash"
        }
      ]
    },
    "user_tbl": {
      "auto_increment":{
	      "column" : "id",
		  "sequence" : "uks.user_seq"
	  },
      "column_vindexes": [
        {
          "column": "region_id",
          "name": "hash"
        }
      ]
    }
  }
}`

	uSchemaSQL = `create table user_seq (
	id int default 0, 
	next_id bigint default null, 
	cache bigint default null, 
	primary key(id)
) comment 'vitess_sequence' Engine=InnoDB;

create table u_tbl(
	id bigint,
	num bigint,
	primary key(id)
) Engine=InnoDB;

insert into user_seq(id, next_id, cache) values (0, 1, 1000);
`

	uVSchema = `
{
  "tables": {
    "u_tbl": {},
    "user_seq": {
       "type":   "sequence"
    }
  }
}`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(Cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start unsharded keyspace
		uKeyspace := &cluster.Keyspace{
			Name:      uKs,
			SchemaSQL: uSchemaSQL,
			VSchema:   uVSchema,
		}
		err = clusterInstance.StartUnshardedKeyspace(*uKeyspace, 0, false)
		if err != nil {
			return 1
		}

		// Start sharded keyspace
		sKeyspace := &cluster.Keyspace{
			Name:      sKs,
			SchemaSQL: sSchemaSQL,
			VSchema:   sVSchema,
		}
		err = clusterInstance.StartKeyspace(*sKeyspace, []string{"-80", "80-"}, 0, false)
		if err != nil {
			return 1
		}

		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}
