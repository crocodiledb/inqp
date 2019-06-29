set enable_incremental to off;
set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;

select
    o_orderkey
from
    orders 
where
    o_orderkey in (261607, 272002, 348353, 446496, 465634, 529796, 554244, 535873)
    and o_orderkey not in (select l_orderkey
                       from lineitem
                       where l_orderkey in (261607, 272002, 348353, 446496, 465634, 529796, 554244, 535873))
--    l_shipmode in ('MAIL')
--    and l_commitdate < l_receiptdate
--    and l_shipdate < l_commitdate
--    and l_receiptdate = date '1994-01-01'


