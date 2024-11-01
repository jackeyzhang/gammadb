create extension gammadb;
--
-- insert with DEFAULT in the target_list
--
create table inserttest (col1 int4, col2 int4 NOT NULL, col3 text default 'testing') using gamma;
insert into inserttest (col1, col2, col3) values (DEFAULT, DEFAULT, DEFAULT);
insert into inserttest (col2, col3) values (3, DEFAULT);
insert into inserttest (col1, col2, col3) values (DEFAULT, 5, DEFAULT);
insert into inserttest values (DEFAULT, 5, 'test');
insert into inserttest values (DEFAULT, 7);

select * from inserttest;

--
-- insert with similar expression / target_list values (all fail)
--
insert into inserttest (col1, col2, col3) values (DEFAULT, DEFAULT);
insert into inserttest (col1, col2, col3) values (1, 2);
insert into inserttest (col1) values (1, 2);
insert into inserttest (col1) values (DEFAULT, DEFAULT);

select * from inserttest;

--
-- VALUES test
--
insert into inserttest values(10, 20, '40'), (-1, 2, DEFAULT),
    ((select 2), (select i from (values(3)) as foo (i)), 'values are fun!');

select * from inserttest;

--
-- TOASTed value test
--
insert into inserttest values(30, 50, repeat('x', 10000));

select col1, col2, char_length(col3) from inserttest;

drop table inserttest;

--
-- tuple larger than fillfactor
--
CREATE TABLE large_tuple_test (a int, b text) using gamma WITH (fillfactor = 10);
ALTER TABLE large_tuple_test ALTER COLUMN b SET STORAGE plain;

-- create page w/ free space in range [nearlyEmptyFreeSpace, MaxHeapTupleSize)
INSERT INTO large_tuple_test (select 1, NULL);

-- should still fit on the page
INSERT INTO large_tuple_test (select 2, repeat('a', 1000));
SELECT pg_size_pretty(pg_relation_size('large_tuple_test'::regclass, 'main'));

-- add small record to the second page
INSERT INTO large_tuple_test (select 3, NULL);

-- now this tuple won't fit on the second page, but the insert should
-- still succeed by extending the relation
INSERT INTO large_tuple_test (select 4, repeat('a', 8126));

DROP TABLE large_tuple_test;

--
-- check indirection (field/array assignment), cf bug #14265
--
-- these tests are aware that transformInsertStmt has 3 separate code paths
--

create type insert_test_type as (if1 int, if2 text[]);

create table inserttest (f1 int, f2 int[],
                         f3 insert_test_type, f4 insert_test_type[]) using gamma;

insert into inserttest (f2[1], f2[2]) values (1,2);
insert into inserttest (f2[1], f2[2]) values (3,4), (5,6);
insert into inserttest (f2[1], f2[2]) select 7,8;
insert into inserttest (f2[1], f2[2]) values (1,default);  -- not supported

insert into inserttest (f3.if1, f3.if2) values (1,array['foo']);
insert into inserttest (f3.if1, f3.if2) values (1,'{foo}'), (2,'{bar}');
insert into inserttest (f3.if1, f3.if2) select 3, '{baz,quux}';
insert into inserttest (f3.if1, f3.if2) values (1,default);  -- not supported

insert into inserttest (f3.if2[1], f3.if2[2]) values ('foo', 'bar');
insert into inserttest (f3.if2[1], f3.if2[2]) values ('foo', 'bar'), ('baz', 'quux');
insert into inserttest (f3.if2[1], f3.if2[2]) select 'bear', 'beer';

insert into inserttest (f4[1].if2[1], f4[1].if2[2]) values ('foo', 'bar');
insert into inserttest (f4[1].if2[1], f4[1].if2[2]) values ('foo', 'bar'), ('baz', 'quux');
insert into inserttest (f4[1].if2[1], f4[1].if2[2]) select 'bear', 'beer';

select * from inserttest;

-- also check reverse-listing
create table inserttest2 (f1 bigint, f2 text) using gamma;
create rule irule1 as on insert to inserttest2 do also
  insert into inserttest (f3.if2[1], f3.if2[2])
  values (new.f1,new.f2);
create rule irule2 as on insert to inserttest2 do also
  insert into inserttest (f4[1].if1, f4[1].if2[2])
  values (1,'fool'),(new.f1,new.f2);
create rule irule3 as on insert to inserttest2 do also
  insert into inserttest (f4[1].if1, f4[1].if2[2])
  select new.f1, new.f2;
\d+ inserttest2

drop table inserttest2;
drop table inserttest;

-- Make the same tests with domains over the array and composite fields

create domain insert_pos_ints as int[] check (value[1] > 0);

create domain insert_test_domain as insert_test_type
  check ((value).if2[1] is not null);

create table inserttesta (f1 int, f2 insert_pos_ints) using gamma;
create table inserttestb (f3 insert_test_domain, f4 insert_test_domain[]) using gamma;

insert into inserttesta (f2[1], f2[2]) values (1,2);
insert into inserttesta (f2[1], f2[2]) values (3,4), (5,6);
insert into inserttesta (f2[1], f2[2]) select 7,8;
insert into inserttesta (f2[1], f2[2]) values (1,default);  -- not supported
insert into inserttesta (f2[1], f2[2]) values (0,2);
insert into inserttesta (f2[1], f2[2]) values (3,4), (0,6);
insert into inserttesta (f2[1], f2[2]) select 0,8;

insert into inserttestb (f3.if1, f3.if2) values (1,array['foo']);
insert into inserttestb (f3.if1, f3.if2) values (1,'{foo}'), (2,'{bar}');
insert into inserttestb (f3.if1, f3.if2) select 3, '{baz,quux}';
insert into inserttestb (f3.if1, f3.if2) values (1,default);  -- not supported
insert into inserttestb (f3.if1, f3.if2) values (1,array[null]);
insert into inserttestb (f3.if1, f3.if2) values (1,'{null}'), (2,'{bar}');
insert into inserttestb (f3.if1, f3.if2) select 3, '{null,quux}';

insert into inserttestb (f3.if2[1], f3.if2[2]) values ('foo', 'bar');
insert into inserttestb (f3.if2[1], f3.if2[2]) values ('foo', 'bar'), ('baz', 'quux');
insert into inserttestb (f3.if2[1], f3.if2[2]) select 'bear', 'beer';

insert into inserttestb (f3, f4[1].if2[1], f4[1].if2[2]) values (row(1,'{x}'), 'foo', 'bar');
insert into inserttestb (f3, f4[1].if2[1], f4[1].if2[2]) values (row(1,'{x}'), 'foo', 'bar'), (row(2,'{y}'), 'baz', 'quux');
insert into inserttestb (f3, f4[1].if2[1], f4[1].if2[2]) select row(1,'{x}')::insert_test_domain, 'bear', 'beer';

select * from inserttesta;
select * from inserttestb;

-- also check reverse-listing
create table inserttest2 (f1 bigint, f2 text) using gamma;
create rule irule1 as on insert to inserttest2 do also
  insert into inserttestb (f3.if2[1], f3.if2[2])
  values (new.f1,new.f2);
create rule irule2 as on insert to inserttest2 do also
  insert into inserttestb (f4[1].if1, f4[1].if2[2])
  values (1,'fool'),(new.f1,new.f2);
create rule irule3 as on insert to inserttest2 do also
  insert into inserttestb (f4[1].if1, f4[1].if2[2])
  select new.f1, new.f2;
\d+ inserttest2

drop table inserttest2;
drop table inserttesta;
drop table inserttestb;
drop domain insert_pos_ints;
drop domain insert_test_domain;

-- Verify that multiple inserts to subfields of a domain-over-container
-- check the domain constraints only on the finished value

create domain insert_nnarray as int[]
  check (value[1] is not null and value[2] is not null);

create domain insert_test_domain as insert_test_type
  check ((value).if1 is not null and (value).if2 is not null);

create table inserttesta (f1 insert_nnarray) using gamma;
insert into inserttesta (f1[1]) values (1);  -- fail
insert into inserttesta (f1[1], f1[2]) values (1, 2);

create table inserttestb (f1 insert_test_domain) using gamma;
insert into inserttestb (f1.if1) values (1);  -- fail
insert into inserttestb (f1.if1, f1.if2) values (1, '{foo}');

drop table inserttesta;
drop table inserttestb;
drop domain insert_nnarray;
drop type insert_test_type cascade;

drop extension gammadb;
