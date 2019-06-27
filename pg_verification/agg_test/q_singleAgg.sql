set enable_incremental to off;
set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set enable_nestloop to on; 
set enable_mergejoin to on;

select
	l_returnflag,
	l_linestatus,
    max(l_quantity) as max_qty,
    min(l_quantity) as min_qty,
    sum(l_extendedprice * (1 - l_discount))
from
	lineitem
where
	l_shipdate <= date '1998-09-01'
group by
	l_returnflag,
	l_linestatus;
