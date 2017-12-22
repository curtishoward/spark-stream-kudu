DROP TABLE IF EXISTS traffic_conditions;

-- This is the syntax for Impala 2.10(CDH 5.13.1)
CREATE TABLE traffic_conditions
(
    as_of_time BIGINT
  , avg_num_veh DOUBLE
  , min_num_veh INT
  , max_num_veh INT
  , first_meas_time BIGINT
  , last_meas_time BIGINT
  , PRIMARY KEY(as_of_time)
)
PARTITION BY HASH PARTITIONS 4
STORED AS KUDU
TBLPROPERTIES
(
    'kudu.table_name' = 'traffic_conditions',
    'kudu.master_addresses' = 'spotch-3.vpc.cloudera.com:7051'
);
