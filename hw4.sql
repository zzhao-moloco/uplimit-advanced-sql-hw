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
top_three_parts AS (
    SELECT
        c_custkey,
        MAX(o_orderdate) AS last_order_date,
        LISTAGG(o_orderkey, ', ') AS order_numbers,
        SUM(l_extendedprice) AS total_spent,
        MAX(CASE WHEN price_row_number = 1 THEN l_partkey END) AS part_1_key,
        MAX(CASE WHEN price_row_number = 1 THEN l_quantity END) AS part_1_quantity,
        MAX(CASE WHEN price_row_number = 1 THEN l_extendedprice END) AS part_1_total_spent,
        MAX(CASE WHEN price_row_number = 2 THEN l_partkey END) AS part_2_key,
        MAX(CASE WHEN price_row_number = 2 THEN l_quantity END) AS part_2_quantity,
        MAX(CASE WHEN price_row_number = 2 THEN l_extendedprice END) AS part_2_total_spent,
        MAX(CASE WHEN price_row_number = 3 THEN l_partkey END) AS part_3_key,
        MAX(CASE WHEN price_row_number = 3 THEN l_quantity END) AS part_3_quantity,
        MAX(CASE WHEN price_row_number = 3 THEN l_extendedprice END) AS part_3_total_spent
    FROM automobile_urgent_customer_parts
    WHERE price_row_number <= 3
    GROUP BY c_custkey
)
SELECT
    c_custkey AS C_CUSTKEY,
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
ORDER BY LAST_ORDER_DATE
LIMIT 100;












-- Review the candidate's tech exercise below, and provide a one-paragraph assessment of the SQL quality. Provide examples/suggestions for improvement if you think the candidate could have chosen a better approach.
-- Do you agree with the results returned by the query?
The results did not agree with my query on the c_custkey.


-- Is it easy to understand?
-- Could the code be more efficient?
