--
-- Regression Tests for Custom Plan APIs
--

create extension gammadb;
-- construction of test data

CREATE TABLE t1 (
    a   int,
	b	double precision
) using gamma;
INSERT INTO t1 SELECT generate_series(1,3), 2.3;
INSERT INTO t1 SELECT generate_series(1,3), 3.3;
INSERT INTO t1 SELECT generate_series(1,3), 4.3;
VACUUM ANALYZE t1;

set work_mem = '100MB';

EXPLAIN (COSTS OFF) SELECT * FROM t1;
SELECT * FROM t1;

EXPLAIN (COSTS OFF) SELECT b FROM t1;
SELECT b FROM t1;

EXPLAIN (COSTS OFF) SELECT b+1 FROM t1;
SELECT b+1 FROM t1;

EXPLAIN (COSTS OFF) SELECT count(b) FROM t1;
SELECT count(b) FROM t1;

set enable_hashagg to on;

EXPLAIN (COSTS OFF) SELECT a, sum(b), avg(b)  FROM t1 group by a;
SELECT a, sum(b), avg(b)  FROM t1 group by a;

EXPLAIN (COSTS OFF) SELECT a, sum(b), avg(b)  FROM t1 where a < 3 group by a;
SELECT a, sum(b), avg(b)  FROM t1 where a < 3 group by a;

set enable_hashagg to off;

EXPLAIN (COSTS OFF) SELECT a, sum(b), avg(b)  FROM t1 group by a;
SELECT a, sum(b), avg(b)  FROM t1 group by a;

EXPLAIN (COSTS OFF) SELECT a, sum(b), avg(b)  FROM t1 where a < 3 group by a;
SELECT a, sum(b), avg(b)  FROM t1 where a < 3 group by a;

reset enable_hashagg;

drop table t1;

drop extension gammadb;
