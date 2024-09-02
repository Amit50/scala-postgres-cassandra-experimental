CREATE KEYSPACE ab_test_keyspace
    WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 2};

use keyspace ab_test_keyspace;

CREATE TABLE sample_test_data_v1 (
    id bigint,
    name text,
    age int,
    salary int,
    update_at timestamp,
    salary_date date,
    PRIMARY KEY (id,name)
);
