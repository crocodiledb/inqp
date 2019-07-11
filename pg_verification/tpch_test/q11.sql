set enable_incremental to off;
set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set enable_nestloop to on; 
set enable_mergejoin to on;

select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'GERMANY'
group by
    ps_partkey 
    having sum(ps_supplycost * ps_availqty) > (
                select
                    sum(ps_supplycost * ps_availqty) * 0.0001 
                from
                    partsupp,
                    supplier,
                    nation
                where
                    ps_suppkey = s_suppkey
                    and s_nationkey = n_nationkey
                    and n_name = 'GERMANY'
            )
order by
value desc

