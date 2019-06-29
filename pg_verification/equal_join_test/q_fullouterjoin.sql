set enable_incremental to off;
set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;

-- select
--     l_shipmode,
--     sum(case
--         when o_orderpriority ='1-URGENT'
--         or o_orderpriority ='2-HIGH'
--         then 1
--         else 0
--         end) as high_line_count,
--     sum(case
--         when o_orderpriority <> '1-URGENT'
--         and o_orderpriority <> '2-HIGH'
--         then 1
--         else 0
--         end) as low_line_count
select
    o_orderkey,
    l_orderkey,
    o_orderpriority
from 
    (select *
     from
        orders
     where
        -- o_orderkey in (75233, 258050, 261607, 272002, 348353, 446496, 465634, 529796, 554244, 535873)
        o_orderkey in (261607, 272002, 348353, 446496, 465634, 529796, 554244, 535873)
    ) filteredO
full join (select *
           from
                lineitem
           where
                l_shipmode in ('MAIL')
                and l_commitdate < l_receiptdate
                and l_shipdate < l_commitdate
                and l_receiptdate = date '1994-01-01') filteredLI
on filteredO.o_orderkey = filteredLI.l_orderkey;
-- group by
--     l_shipmode;

