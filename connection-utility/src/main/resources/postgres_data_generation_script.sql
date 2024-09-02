create index emp_indx on table1(id) include(col1);
CLUSTER table1 USING emp_indx;
-- display indexes
SELECT *
FROM pg_indexes
WHERE schemaname = 'public';

vacuum (analyze, verbose, full);
VACUUM FULL sample_dataset_range;


-- partitioned table
CREATE TABLE sample_dataset_range (
    id SERIAL,
    name text not null,
    age int not null,
    salary int not null,
    primary key (id,age)
) partition by range (age);


CREATE TABLE sample_dataset_range_0_10 PARTITION OF sample_dataset_range
FOR VALUES FROM (0) TO (11);
CREATE TABLE sample_dataset_range_11_20 PARTITION OF sample_dataset_range
FOR VALUES FROM (11) TO (21);
CREATE TABLE sample_dataset_range_21_30 PARTITION OF sample_dataset_range
FOR VALUES FROM (21) TO (31);
CREATE TABLE sample_dataset_range_31_45 PARTITION OF sample_dataset_range
FOR VALUES FROM (31) TO (46);
CREATE TABLE sample_dataset_range_46_60 PARTITION OF sample_dataset_range
FOR VALUES FROM (46) TO (61);
CREATE TABLE sample_dataset_range_above_61 PARTITION OF sample_dataset_range
FOR VALUES FROM (61) TO (MAXVALUE);

drop table public.sample_dataset_range_v2;
CREATE TABLE public.sample_dataset_range_v2 (
	id int8 NOT null,
	"name" text NULL,
	age int4 NULL,
	salary int4 NULL,
	updated_at timestamp NOT NULL,
	created_at timestamp NOT NULL,
	salary_date date null,
	CONSTRAINT sample_dataset_range_v2_pkey PRIMARY KEY (id)
);
