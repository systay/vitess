/*
Copyright 2024 The Vitess Authors.

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

package union

import (
	"fmt"
	"golang.org/x/exp/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func start(t testing.TB) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, mcmp.VtConn, "set workload = oltp")

		tables := []string{"nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem"}
		for _, table := range tables {
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
		}
	}

	deleteAll()

	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, keyspaceName, "region", `R_COMMENT`)
	require.NoError(t, err)

	// Set the size parameter here. Increase for more data.
	size := 100

	// Seed the random number generator
	rand.Seed(12345)

	// Generate dynamic data
	regions := generateRegions()
	nations := generateNations(size, regions)
	suppliers := generateSuppliers(size, nations)
	parts := generateParts(size)
	customers := generateCustomers(size, nations)
	orders := generateOrders(size, customers)
	lineitems := generateLineItems(orders, parts, suppliers)

	// Execute inserts
	mcmp.Exec(buildInsertQuery("region", regions))
	mcmp.Exec(buildInsertQuery("nation", nations))
	mcmp.Exec(buildInsertQuery("supplier", suppliers))
	mcmp.Exec(buildInsertQuery("part", parts))
	mcmp.Exec(buildInsertQuery("customer", customers))
	mcmp.Exec(buildInsertQuery("orders", orders))
	mcmp.Exec(buildInsertQuery("lineitem", lineitems))

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
		cluster.PanicHandler(t)
	}
}

func TestTPCHQueries(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	err := utils.WaitForColumn(t, clusterInstance.VtgateProcess, keyspaceName, "region", `R_COMMENT`)
	require.NoError(t, err)

	insertQueries := []string{
		`INSERT INTO region (R_REGIONKEY, R_NAME, R_COMMENT) VALUES
	(1, 'ASIA', 'Eastern Asia'),
	(2, 'MIDDLE EAST', 'Rich cultural heritage');`,
		`INSERT INTO nation (N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT) VALUES
	(1, 'China', 1, 'Large population'),
	(2, 'India', 1, 'Large variety of cultures'),
	(3, 'Nation A', 2, 'Historic sites'),
	(4, 'Nation B', 2, 'Beautiful landscapes');`,
		`INSERT INTO supplier (S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT) VALUES
	(1, 'Supplier A', '123 Square', 1, '86-123-4567', 5000.00, 'High quality steel'),
	(2, 'Supplier B', '456 Ganges St', 2, '91-789-4561', 5500.00, 'Efficient production'),
	(3, 'Supplier 1', 'Supplier Address 1', 3, '91-789-4562', 3000.00, 'Supplier Comment 1'),
	(4, 'Supplier 2', 'Supplier Address 2', 2, '91-789-4563', 4000.00, 'Supplier Comment 2');`,
		`INSERT INTO part (P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT) VALUES
	(100, 'Part 100', 'MFGR A', 'Brand X', 'BOLT STEEL', 30, 'SM BOX', 45.00, 'High strength'),
	(101, 'Part 101', 'MFGR B', 'Brand Y', 'NUT STEEL', 30, 'LG BOX', 30.00, 'Rust resistant');`,
		`INSERT INTO partsupp (PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT) VALUES
	(100, 1, 500, 10.00, 'Deliveries on time'),
	(101, 2, 300, 9.00, 'Back orders possible'),
	(100, 2, 600, 8.50, 'Bulk discounts available');`,
		`INSERT INTO customer (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT) VALUES
	(1, 'Customer A', '1234 Drive Lane', 1, '123-456-7890', 1000.00, 'AUTOMOBILE', 'Frequent orders'),
	(2, 'Customer B', '5678 Park Ave', 2, '234-567-8901', 2000.00, 'AUTOMOBILE', 'Large orders'),
	(3, 'Customer 1', 'Address 1', 1, 'Phone 1', 1000.00, 'Segment 1', 'Comment 1'),
	(4, 'Customer 2', 'Address 2', 2, 'Phone 2', 2000.00, 'Segment 2', 'Comment 2');`,
		`INSERT INTO orders (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT) VALUES
	(100, 1, 'O', 15000.00, '1995-03-10', '1-URGENT', 'Clerk#0001', 1, 'N/A'),
	(101, 2, 'O', 25000.00, '1995-03-05', '2-HIGH', 'Clerk#0002', 2, 'N/A'),
	(1, 3, 'O', 10000.00, '1994-01-10', 'Priority 1', 'Clerk 1', 1, 'Order Comment 1'),
	(2, 4, 'O', 20000.00, '1994-06-15', 'Priority 2', 'Clerk 2', 1, 'Order Comment 2');`,
		`INSERT INTO lineitem (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT) VALUES
	(100, 200, 300, 1, 10, 5000.00, 0.05, 0.10, 'N', 'O', '1995-03-15', '1995-03-14', '1995-03-16', 'DELIVER IN PERSON', 'TRUCK', 'Urgent delivery'),
	(100, 201, 301, 2, 20, 10000.00, 0.10, 0.10, 'R', 'F', '1995-03-17', '1995-03-15', '1995-03-18', 'NONE', 'MAIL', 'Handle with care'),
	(101, 202, 302, 1, 30, 15000.00, 0.00, 0.10, 'A', 'F', '1995-03-20', '1995-03-18', '1995-03-21', 'TAKE BACK RETURN', 'SHIP', 'Standard delivery'),
	(101, 203, 303, 2, 40, 10000.00, 0.20, 0.10, 'N', 'O', '1995-03-22', '1995-03-20', '1995-03-23', 'DELIVER IN PERSON', 'RAIL', 'Expedite'),
	(1, 101, 1, 1, 5, 5000.00, 0.1, 0.05, 'N', 'O', '1994-01-12', '1994-01-11', '1994-01-13', 'Deliver in person','TRUCK', 'Lineitem Comment 1'),
	(2, 102, 2, 1, 3, 15000.00, 0.2, 0.05, 'R', 'F', '1994-06-17', '1994-06-15', '1994-06-18', 'Leave at front door','AIR', 'Lineitem Comment 2'),
	(11, 100, 2, 1, 30, 10000.00, 0.05, 0.07, 'A', 'F', '1998-07-21', '1998-07-22', '1998-07-23', 'DELIVER IN PERSON', 'TRUCK', 'N/A'),
	(12, 101, 3, 1, 50, 15000.00, 0.10, 0.08, 'N', 'O', '1998-08-10', '1998-08-11', '1998-08-12', 'NONE', 'AIR', 'N/A'),
	(13, 102, 4, 1, 70, 21000.00, 0.02, 0.04, 'R', 'F', '1998-06-30', '1998-07-01', '1998-07-02', 'TAKE BACK RETURN', 'MAIL', 'N/A'),
	(14, 103, 5, 1, 90, 30000.00, 0.15, 0.10, 'A', 'O', '1998-05-15', '1998-05-16', '1998-05-17', 'DELIVER IN PERSON', 'RAIL', 'N/A'),
	(15, 104, 2, 1, 45, 45000.00, 0.20, 0.15, 'N', 'F', '1998-07-15', '1998-07-16', '1998-07-17', 'NONE', 'SHIP', 'N/A');`,
	}

	for _, query := range insertQueries {
		mcmp.Exec(query)
	}

	testcases := []struct {
		name  string
		query string
	}{
		{
			name: "Q1",
			query: `select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date_sub('1998-12-01', interval 108 day)
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;`,
		},
		{
			name: "Q11",
			query: `select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as value
from
	partsupp,
	supplier,
	nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'MOZAMBIQUE'
group by
	ps_partkey having
		sum(ps_supplycost * ps_availqty) > (
			select
				sum(ps_supplycost * ps_availqty) * 0.0001000000
			from
				partsupp,
				supplier,
				nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = 'MOZAMBIQUE'
		)
order by
	value desc;`,
		},
		{
			name: "Q14 without decimal literal",
			query: `select sum(case
               when p_type like 'PROMO%'
                   then l_extendedprice * (1 - l_discount)
               else 0
    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from lineitem,
     part
where l_partkey = p_partkey
  and l_shipdate >= '1996-12-01'
  and l_shipdate < date_add('1996-12-01', interval '1' month);`,
		},
		{
			name: "Q14 without case",
			query: `select 100.00 * sum(l_extendedprice * (1 - l_discount)) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from lineitem,
     part
where l_partkey = p_partkey
  and l_shipdate >= '1996-12-01'
  and l_shipdate < date_add('1996-12-01', interval '1' month);`,
		},
		{
			name: "Q14",
			query: `select 100.00 * sum(case
                        when p_type like 'PROMO%'
                            then l_extendedprice * (1 - l_discount)
                        else 0
    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from lineitem,
     part
where l_partkey = p_partkey
  and l_shipdate >= '1996-12-01'
  and l_shipdate < date_add('1996-12-01', interval '1' month);`,
		},
		{
			name: "Q8",
			query: `select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
from (select extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation
      from part,
           supplier,
           lineitem,
           orders,
           customer,
           nation n1,
           nation n2,
           region
      where p_partkey = l_partkey
        and s_suppkey = l_suppkey
        and l_orderkey = o_orderkey
        and o_custkey = c_custkey
        and c_nationkey = n1.n_nationkey
        and n1.n_regionkey = r_regionkey
        and r_name = 'AMERICA'
        and s_nationkey = n2.n_nationkey
        and o_orderdate between date '1995-01-01' and date ('1996-12-31') and p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations
group by o_year
order by o_year`,
		},
		{
			name: "simple derived table",
			query: `select *
from (select l.l_extendedprice * o.o_totalprice
      from lineitem l
               join orders o) as dt`,
		},
	}

	for _, testcase := range testcases {
		mcmp.Run(testcase.name, func(mcmp *utils.MySQLCompare) {
			mcmp.Exec(testcase.query)
		})
	}
}

func BenchmarkQuery(b *testing.B) {
	mcmp, closer := start(b)
	defer closer()

	for i := 0; i < b.N; i++ {
		_ = utils.Exec(b, mcmp.VtConn, "vexplain trace "+q)
	}
}

const q = `SELECT
    o.o_orderpriority,
    EXTRACT(YEAR FROM o.o_orderdate) AS order_year,
    COUNT(DISTINCT o.o_orderkey) AS order_count,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue
FROM
    orders o
JOIN
    lineitem l ON o.o_orderkey > l.l_orderkey
WHERE
    o.o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'
GROUP BY
    o.o_orderpriority,
    EXTRACT(YEAR FROM o.o_orderdate)
ORDER BY
    o.o_orderpriority,
    order_year`

func TestVexplain(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	err := utils.WaitForColumn(t, clusterInstance.VtgateProcess, keyspaceName, "region", `R_COMMENT`)
	require.NoError(t, err)
	fmt.Println(vtParams.Port)
	fmt.Println(vtParams.Host)

	res := utils.Exec(t, mcmp.VtConn, "vexplain trace "+q)
	fmt.Printf("Query: %s\n", q)
	fmt.Printf("Result: %s\n", res.Rows[0][0].ToString())
}

func generateRegions() [][]interface{} {
	regions := [][]interface{}{
		{1, "AMERICA", "New World"},
		{2, "ASIA", "Eastern Asia"},
		{3, "EUROPE", "Old World"},
		{4, "AFRICA", "Dark Continent"},
		{5, "AUSTRALIA", "Down Under"},
	}
	return regions
}

func generateNations(size int, regions [][]interface{}) [][]interface{} {
	var nations [][]interface{}
	for i := 0; i < size/5; i++ {
		for _, region := range regions {
			nationKey := len(nations) + 1
			regionKey := region[0].(int)
			name := fmt.Sprintf("Nation_%d_%d", regionKey, i)
			if regionKey == 1 && i == 0 {
				name = "BRAZIL"
			}
			nations = append(nations, []interface{}{nationKey, name, regionKey, fmt.Sprintf("Comment for %s", name)})
		}
	}
	return nations
}

func generateSuppliers(size int, nations [][]interface{}) [][]interface{} {
	var suppliers [][]interface{}
	for i := 0; i < size; i++ {
		nation := nations[rand.Intn(len(nations))]
		suppliers = append(suppliers, []interface{}{
			i + 1,
			fmt.Sprintf("Supplier_%d", i+1),
			fmt.Sprintf("Address_%d", i+1),
			nation[0],
			fmt.Sprintf("%d-123-4567", rand.Intn(100)),
			float64(rand.Intn(10000)) + rand.Float64(),
			fmt.Sprintf("Comment for Supplier_%d", i+1),
		})
	}
	return suppliers
}

func generateParts(size int) [][]interface{} {
	var parts [][]interface{}
	types := []string{"ECONOMY ANODIZED STEEL", "LARGE BRUSHED BRASS", "STANDARD POLISHED COPPER", "SMALL PLATED STEEL", "MEDIUM BURNISHED TIN"}
	for i := 0; i < size; i++ {
		parts = append(parts, []interface{}{
			i + 1,
			fmt.Sprintf("Part_%d", i+1),
			fmt.Sprintf("Manufacturer_%d", rand.Intn(5)+1),
			fmt.Sprintf("Brand_%d", rand.Intn(5)+1),
			types[rand.Intn(len(types))],
			rand.Intn(50) + 1,
			fmt.Sprintf("%s BOX", []string{"SM", "LG", "MED", "JUMBO", "WRAP"}[rand.Intn(5)]),
			float64(rand.Intn(1000)) + rand.Float64(),
			fmt.Sprintf("Comment for Part_%d", i+1),
		})
	}
	return parts
}

func generateCustomers(size int, nations [][]interface{}) [][]interface{} {
	var customers [][]interface{}
	for i := 0; i < size; i++ {
		nation := nations[rand.Intn(len(nations))]
		customers = append(customers, []interface{}{
			i + 1,
			fmt.Sprintf("Customer_%d", i+1),
			fmt.Sprintf("Address_%d", i+1),
			nation[0],
			fmt.Sprintf("%d-987-6543", rand.Intn(100)),
			float64(rand.Intn(10000)) + rand.Float64(),
			[]string{"AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"}[rand.Intn(5)],
			fmt.Sprintf("Comment for Customer_%d", i+1),
		})
	}
	return customers
}

func generateOrders(size int, customers [][]interface{}) [][]interface{} {
	var orders [][]interface{}
	startDate := time.Date(1995, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(1996, 12, 31, 0, 0, 0, 0, time.UTC)
	for i := 0; i < size*10; i++ {
		customer := customers[rand.Intn(len(customers))]
		orderDate := startDate.Add(time.Duration(rand.Int63n(int64(endDate.Sub(startDate)))))
		orders = append(orders, []interface{}{
			i + 1,
			customer[0],
			[]string{"O", "F", "P"}[rand.Intn(3)],
			float64(rand.Intn(100000)) + rand.Float64(),
			orderDate.Format("2006-01-02"),
			fmt.Sprintf("%d-URGENT", rand.Intn(5)+1),
			fmt.Sprintf("Clerk#%05d", rand.Intn(1000)),
			rand.Intn(5),
			fmt.Sprintf("Comment for Order_%d", i+1),
		})
	}
	return orders
}

func generateLineItems(orders [][]interface{}, parts [][]interface{}, suppliers [][]interface{}) [][]interface{} {
	var lineItems [][]interface{}
	for _, order := range orders {
		for j := 0; j < rand.Intn(7)+1; j++ {
			part := parts[rand.Intn(len(parts))]
			supplier := suppliers[rand.Intn(len(suppliers))]
			orderDate, _ := time.Parse("2006-01-02", order[4].(string))
			shipDate := orderDate.Add(time.Duration(rand.Intn(30)) * 24 * time.Hour)
			commitDate := orderDate.Add(time.Duration(rand.Intn(30)) * 24 * time.Hour)
			receiptDate := shipDate.Add(time.Duration(rand.Intn(30)) * 24 * time.Hour)
			lineItems = append(lineItems, []interface{}{
				order[0],
				part[0],
				supplier[0],
				j + 1,
				rand.Intn(50) + 1,
				float64(rand.Intn(100000)) + rand.Float64(),
				rand.Float64(),
				rand.Float64(),
				[]string{"N", "R", "A"}[rand.Intn(3)],
				[]string{"O", "F"}[rand.Intn(2)],
				shipDate.Format("2006-01-02"),
				commitDate.Format("2006-01-02"),
				receiptDate.Format("2006-01-02"),
				[]string{"DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"}[rand.Intn(4)],
				[]string{"TRUCK", "MAIL", "RAIL", "AIR", "SHIP"}[rand.Intn(5)],
				fmt.Sprintf("Comment for Lineitem_%d_%d", order[0], j+1),
			})
		}
	}
	return lineItems
}

func buildInsertQuery(tableName string, data [][]interface{}) string {
	if len(data) == 0 {
		return ""
	}
	columns := getColumns(tableName)
	valueStrings := make([]string, 0, len(data))
	for _, row := range data {
		valueStrings = append(valueStrings, formatRow(row))
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tableName, strings.Join(columns, ", "), strings.Join(valueStrings, ",\n"))
	return query
}

func getColumns(tableName string) []string {
	switch tableName {
	case "region":
		return []string{"R_REGIONKEY", "R_NAME", "R_COMMENT"}
	case "nation":
		return []string{"N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"}
	case "supplier":
		return []string{"S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT"}
	case "part":
		return []string{"P_PARTKEY", "P_NAME", "P_MFGR", "P_BRAND", "P_TYPE", "P_SIZE", "P_CONTAINER", "P_RETAILPRICE", "P_COMMENT"}
	case "customer":
		return []string{"C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT"}
	case "orders":
		return []string{"O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE", "O_ORDERDATE", "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT"}
	case "lineitem":
		return []string{"L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER", "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX", "L_RETURNFLAG", "L_LINESTATUS", "L_SHIPDATE", "L_COMMITDATE", "L_RECEIPTDATE", "L_SHIPINSTRUCT", "L_SHIPMODE", "L_COMMENT"}
	default:
		return []string{}
	}
}

func formatRow(row []interface{}) string {
	values := make([]string, len(row))
	for i, v := range row {
		switch v := v.(type) {
		case string:
			values[i] = fmt.Sprintf("'%s'", strings.Replace(v, "'", "''", -1))
		case float64:
			values[i] = fmt.Sprintf("%.2f", v)
		default:
			values[i] = fmt.Sprintf("%v", v)
		}
	}
	return "(" + strings.Join(values, ", ") + ")"
}
