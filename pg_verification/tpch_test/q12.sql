set enable_incremental to off;
set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;

select
    l_shipmode,
    sum(case
        when o_orderpriority ='1-URGENT'
        or o_orderpriority ='2-HIGH'
        then 1
        else 0
        end) as high_line_count,
    sum(case
        when o_orderpriority <> '1-URGENT'
        and o_orderpriority <> '2-HIGH'
        then 1
        else 0
        end) as low_line_count
from
    orders,
    lineitem
where
    o_orderkey = l_orderkey
    and l_shipmode in ('MAIL')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= date '1994-01-01'
    and l_receiptdate < date '1994-01-02'
group by
    l_shipmode
order by
    l_shipmode;
