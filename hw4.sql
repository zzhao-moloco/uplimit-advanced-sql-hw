-- We need to develop a report to analyze AUTOMOBILE customers who have placed URGENT orders. We expect to see one row per customer, with the following columns:
-- C_CUSTKEY
-- LAST_ORDER_DATE: The date when the last URGENT order was placed
-- ORDER_NUMBERS: A comma-separated list of the order_keys for the three highest dollar urgent orders
-- TOTAL_SPENT: The total dollar amount of the three highest orders
-- PART_1_KEY: The identifier for the part with the highest dollar amount spent, across all urgent orders 
-- PART_1_QUANTITY: The quantity ordered
-- PART_1_TOTAL_SPENT: Total dollars spent on the part 
-- PART_2_KEY: The identifier for the part with the second-highest dollar amount spent, across all urgent orders  
-- PART_2_QUANTITY: The quantity ordered
-- PART_2_TOTAL_SPENT: Total dollars spent on the part 
-- PART_3_KEY: The identifier for the part with the third-highest dollar amount spent, across all urgent orders 
-- PART_3_QUANTITY: The quantity ordered
-- PART_3_TOTAL_SPENT: Total dollars spent on the part 
-- The output should be sorted by LAST_ORDER_DATE descending.
-- Create a query to provide the report requested. Your query should have a LIMIT 100 when you submit it for review. Remember that you are creating this as a tech exercise for a job evaluation. Your query should be well-formatted, with clear names and comments.

-- select * from snowflake_sample_data.tpch_sf1.orders limit 10;
-- select * from snowflake_sample_data.tpch_sf1.customer limit 10;
-- select * from snowflake_sample_data.tpch_sf1.lineitem limit 10;


-- Steps
-- find urgent, automobile line items from automobile customers by joining customers, orders, lineitems and get row number
-- after select row number 1,2 and 3 rows, get max key, quantity and part price 
-- sort order by desc last_order_date and limit to 100
-- show columns in table orders;
-- show columns in table lineitem;

WITH automobile_urgent_customer_parts AS (
    SELECT
        c_custkey::string as customer_key,
        o_orderkey::string as order_key,
        l_partkey,
        o_orderdate,
        l_extendedprice,
        l_quantity,
        RANK() OVER (PARTITION BY customer_key ORDER BY l_extendedprice DESC) AS price_rank
    FROM snowflake_sample_data.tpch_sf1.customer
    INNER JOIN snowflake_sample_data.tpch_sf1.orders
        ON customer_key = orders.o_custkey::string
    INNER JOIN snowflake_sample_data.tpch_sf1.lineitem
        ON orders.o_orderkey = lineitem.l_orderkey
    WHERE
        c_mktsegment = 'AUTOMOBILE'
        AND o_orderpriority = '1-URGENT'
),
top_three_parts AS (
    SELECT
        customer_key,
        MAX(o_orderdate) AS last_order_date,
        LISTAGG(order_key, ', ') AS order_numbers,
        SUM(l_extendedprice) AS total_spent,
        MAX(CASE WHEN price_rank = 1 THEN l_partkey END) AS part_1_key,
        MAX(CASE WHEN price_rank = 1 THEN l_quantity END) AS part_1_quantity,
        MAX(CASE WHEN price_rank = 1 THEN l_extendedprice END) AS part_1_total_spent,
        MAX(CASE WHEN price_rank = 2 THEN l_partkey END) AS part_2_key,
        MAX(CASE WHEN price_rank = 2 THEN l_quantity END) AS part_2_quantity,
        MAX(CASE WHEN price_rank = 2 THEN l_extendedprice END) AS part_2_total_spent,
        MAX(CASE WHEN price_rank = 3 THEN l_partkey END) AS part_3_key,
        MAX(CASE WHEN price_rank = 3 THEN l_quantity END) AS part_3_quantity,
        MAX(CASE WHEN price_rank = 3 THEN l_extendedprice END) AS part_3_total_spent
    FROM automobile_urgent_customer_parts
    WHERE price_rank <= 3
    GROUP BY customer_key
)
SELECT
    customer_key AS C_CUSTKEY,
    last_order_date AS LAST_ORDER_DATE,
    order_numbers AS ORDER_NUMBERS,
    total_spent AS TOTAL_SPENT,
    part_1_key AS PART_1_KEY,
    part_1_quantity AS PART_1_QUANTITY,
    part_1_total_spent AS PART_1_TOTAL_SPENT,
    part_2_key AS PART_2_KEY,
    part_2_quantity AS PART_2_QUANTITY,
    part_2_total_spent AS PART_2_TOTAL_SPENT,
    part_3_key AS PART_3_KEY,
    part_3_quantity AS PART_3_QUANTITY,
    part_3_total_spent AS PART_3_TOTAL_SPENT
FROM top_three_parts
ORDER BY LAST_ORDER_DATE DESC
LIMIT 100;



-- Review the candidate's tech exercise below, and provide a one-paragraph assessment of the SQL quality. Provide examples/suggestions for improvement if you think the candidate could have chosen a better approach.
-- Do you agree with the results returned by the query?

-- The results between my query and candiate query are different. I believe it comes from the candiate not parsing the customer key into string format and the usage of row_number() instead of rank() for computing rank.

-- Is it easy to understand?

-- The candiate query joined top_orders with urgent_orders several times in order to generate the part_1_key, part_2_key and part_3_key. This was not simply to understand.

-- Could the code be more efficient?

-- Yes, it could be more efficient. In CTE producing urgent_orders, parts table was not really needed.
-- Case when can by used instead of performing multiple joins from top_orders to urgent_orders and using WHERE clause to select the columns with top 3 ranks. The jions are also not using ON clause to filter but instead used WHERE after the join.

-- Candiate solution:

-- with urgent_orders as (
--     select
--     	o_orderkey,
--     	o_orderdate,
--         c_custkey,
--         p_partkey,
--         l_quantity,
--         l_extendedprice,
--         row_number() over (partition by c_custkey order by l_extendedprice desc) as price_rank
--     from snowflake_sample_data.tpch_sf1.orders as o
--     inner join snowflake_sample_data.tpch_sf1.customer as c on o.o_custkey = c.c_custkey
--     inner join snowflake_sample_data.tpch_sf1.lineitem as l on o.o_orderkey = l.l_orderkey
--     inner join snowflake_sample_data.tpch_sf1.part as p on l.l_partkey = p.p_partkey
--     where c.c_mktsegment = 'AUTOMOBILE'
--     	and o.o_orderpriority = '1-URGENT'
--     order by 1, 2),

-- top_orders as (
--     select
--     	c_custkey,
--         max(o_orderdate) as last_order_date,
--         listagg(o_orderkey, ', ') as order_numbers,
--         sum(l_extendedprice) as total_spent
--     from urgent_orders
--     where price_rank <= 3
--     group by 1
--     order by 1)

-- select 
-- 	t.c_custkey,
--     t.last_order_date,
--     t.order_numbers,
--     t.total_spent,
--     u.p_partkey as part_1_key,
--     u.l_quantity as part_1_quantity,
--     u.l_extendedprice as part_1_total_spent,
--     u2.p_partkey as part_2_key,
--     u2.l_quantity as part_2_quantity,
--     u2.l_extendedprice as part_2_total_spent,
--     u3.p_partkey as part_3_key,
--     u3.l_quantity as part_3_quantity,
--     u3.l_extendedprice as part_3_total_spent
-- from top_orders as t
-- inner join urgent_orders as u on t.c_custkey = u.c_custkey
-- inner join urgent_orders as u2 on t.c_custkey = u2.c_custkey
-- inner join urgent_orders as u3 on t.c_custkey = u3.c_custkey
-- where u.price_rank = 1 and u2.price_rank = 2 and u3.price_rank = 3
-- order by t.last_order_date desc
-- limit 100
