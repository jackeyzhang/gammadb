--
-- Regression Tests for Custom Plan APIs
--

create extension gammadb;
-- construction of test data

CREATE TABLE t1 (
    c1   int,
    c2   int,
    c3   int,
    c4   int
) using gamma;
INSERT INTO t1 SELECT i%7,i%13,i%19,i%23 from generate_series(1,10000) i;
VACUUM ANALYZE t1;

set work_mem = '100MB';

EXPLAIN (COSTS OFF) SELECT count(c1) FROM t1;
SELECT count(c1) FROM t1;

set enable_hashagg to on;

EXPLAIN (COSTS OFF) SELECT c4, sum(c2) FROM t1 group by c4;
SELECT c4, sum(c2)  FROM t1 group by c4;

EXPLAIN (COSTS OFF) SELECT c4, sum(c2) FROM t1 where c3 >3 group by c4;
SELECT c4, sum(c2)  FROM t1 where c3 >3 group by c4;

EXPLAIN (COSTS OFF) SELECT sum(c1), count(distinct c4) FROM t1 group by c3,c2;
SELECT sum(c1), count(distinct c4) FROM t1 group by c3,c2;

EXPLAIN (COSTS OFF) SELECT sum(c1), count(distinct c4) FROM t1 group by rollup(c3,c2);
SELECT sum(c1), count(distinct c4) FROM t1 group by rollup(c3,c2);

set enable_hashagg to off;

EXPLAIN (COSTS OFF) SELECT c4, sum(c2) FROM t1 group by c4;
SELECT c4, sum(c2)  FROM t1 group by c4;

EXPLAIN (COSTS OFF) SELECT c4, sum(c2) FROM t1 where c3 >3 group by c4;
SELECT c4, sum(c2)  FROM t1 where c3 >3 group by c4;

reset enable_hashagg;

drop table t1;

drop extension gammadb;
