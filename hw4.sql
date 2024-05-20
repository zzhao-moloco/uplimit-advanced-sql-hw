WITH automobile_urgent_customer_parts AS (
    SELECT
        c.c_custkey,
        o.o_orderkey,
        l.l_partkey,
        o.o_orderdate,
        l.l_extendedprice,
        l.l_quantity,
        ROW_NUMBER() OVER (PARTITION BY c.c_custkey ORDER BY l.l_extendedprice DESC) AS price_row_number
    FROM snowflake_sample_data.tpch_sf1.customer c
    INNER JOIN snowflake_sample_data.tpch_sf1.orders o
        ON c.c_custkey = o.o_custkey
    INNER JOIN snowflake_sample_data.tpch_sf1.lineitem l
        ON o.o_orderkey = l.l_orderkey
    WHERE
        c.c_mktsegment = 'AUTOMOBILE'
        AND o.o_orderpriority = '1-URGENT'
),
top_three_part_key_quantity_spent as (
select
    c_custkey,
    max(o_orderdate) as last_order_date,
    listagg(o_orderkey, ', ') as order_numbers,
    sum(l_extendedprice) as total_spent,
    max(case when price_row_number = 1 then l_partkey end) as part_1_key,
    max(case when price_row_number = 1 then l_quantity end) as part_1_quantity,
    max(case when price_row_number = 1 then l_extendedprice end) as part_1_total_spent,
    max(case when price_row_number = 2 then l_partkey end) as part_2_key,
    max(case when price_row_number = 2 then l_quantity end) as part_2_quantity,
    max(case when price_row_number = 2 then l_extendedprice end) as part_2_total_spent,
    max(case when price_row_number = 3 then l_partkey end) as part_3_key,
    max(case when price_row_number = 3 then l_quantity end) as part_3_quantity,
    max(case when price_row_number = 3 then l_extendedprice end) as part_3_total_spent
from automobile_urgent_customer_parts
where price_row_number <= 3
group by c_custkey
)
select
    c_custkey as C_CUSTKEY,
    last_order_date as LAST_ORDER_DATE,
    order_numbers as ORDER_NUMBERS,
    total_spent as TOTAL_SPENT,
    part_1_key as PART_1_KEY,
    part_1_quantity as PART_1_QUANTITY,
    part_1_total_spent as PART_1_TOTAL_SPENT,
    part_2_key as PART_2_KEY,
    part_2_quantity as PART_2_QUANTITY,
    part_2_total_spent as PART_2_TOTAL_SPENT,
    part_3_key as PART_3_KEY,
    part_3_quantity as PART_3_QUANTITY,
    part_3_total_spent as PART_3_TOTAL_SPENT
from top_three_part_key_quantity_spent
order by LAST_ORDER_DATE
limit 100