-- Virtual Kitchen has an emergency! 

-- We shipped several meal kits without including fresh parsley, and our customers are starting to complain. We have identified the impacted cities, and we know that 25 of our customers did not get their parsley. That number might seem small, but Virtual Kitchen is committed to providing every customer with a great experience.

-- Our management has decided to provide a different recipe for free (if the customer has other preferences available), or else use grocery stores in the greater Chicago area to send an overnight shipment of fresh parsley to our customers. We have one store in Chicago, IL and one store in Gary, IN both ready to help out with this request.

-- Last night, our on-call developer created a query to identify the impacted customers and their attributes in order to compose an offer to these customers to make things right. But the developer was paged at 2 a.m. when the problem occurred, and she created a fast query so that she could go back to sleep.

-- You review her code today and decide to reformat her query so that she can catch up on sleep.

-- Here is the query she emailed you. Refactor it to apply a consistent format, and add comments that explain your choices. We are going to review different options in the lecture, so if you are willing to share your refactored query with the class, then let us know!

-- select 
--     first_name || ' ' || last_name as customer_name,
--     ca.customer_city,
--     ca.customer_state,
--     s.food_pref_count,
--     (st_distance(us.geo_location, chic.geo_location) / 1609)::int as chicago_distance_miles,
--     (st_distance(us.geo_location, gary.geo_location) / 1609)::int as gary_distance_miles
-- from vk_data.customers.customer_address as ca
-- join vk_data.customers.customer_data c on ca.customer_id = c.customer_id
-- left join vk_data.resources.us_cities us 
-- on UPPER(rtrim(ltrim(ca.customer_state))) = upper(TRIM(us.state_abbr))
--     and trim(lower(ca.customer_city)) = trim(lower(us.city_name))
-- join (
--     select 
--         customer_id,
--         count(*) as food_pref_count
--     from vk_data.customers.customer_survey
--     where is_active = true
--     group by 1
-- ) s on c.customer_id = s.customer_id
--     cross join 
--     ( select 
--         geo_location
--     from vk_data.resources.us_cities 
--     where city_name = 'CHICAGO' and state_abbr = 'IL') chic
-- cross join 
--     ( select 
--         geo_location
--     from vk_data.resources.us_cities 
--     where city_name = 'GARY' and state_abbr = 'IN') gary
-- where 
--     ((trim(city_name) ilike '%concord%' or trim(city_name) ilike '%georgetown%' or trim(city_name) ilike '%ashland%')
--     and customer_state = 'KY')
--     or
--     (customer_state = 'CA' and (trim(city_name) ilike '%oakland%' or trim(city_name) ilike '%pleasant hill%'))
--     or
--     (customer_state = 'TX' and ((trim(city_name) ilike '%arlington%') or trim(city_name) ilike '%brownsville%'))


-- Refactor
-- Step 1 : move subqueries to CTEs
-- Step 2 : break down the main query into smaller helper CTEs like us_cities, customers
-- Step 3 : standardize the trim and uppercase string operation of city names and state abbreviations
-- Step 3 : re-order the join in the main query to start from filtered CTEs like impacted_customer_address and active_pref_customers
-- Step 4 : round the geometry st_distance output to a fixed precision.

WITH active_pref_customers AS (
SELECT 
    customer_id,
    count(*) AS food_pref_count
FROM vk_data.customers.customer_survey
WHERE is_active = true
GROUP BY customer_id
), 
chicago_geo_location AS (
SELECT
    geo_location
FROM vk_data.resources.us_cities 
WHERE city_name = 'CHICAGO' AND state_abbr = 'IL'
),
gary_geo_location AS ( 
SELECT 
    geo_location
FROM vk_data.resources.us_cities 
WHERE city_name = 'GARY' AND state_abbr = 'IN'
),
impacted_customer_address AS (
SELECT
    customer_id,
    upper(trim(customer_city)) AS city,
    upper(trim(customer_state)) AS state
FROM vk_data.customers.customer_address
WHERE
    (state = 'KY' and (city ILIKE ANY ('%concord%','%georgetown%','%ashland%')))
    or
    (state = 'CA' and (city ILIKE ANY ('%oakland%', '%pleasant hill%')))
    or
    (state = 'TX' and (city ILIKE ANY ('%arlington%', '%brownsville%')))
),
customers AS (
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
FROM vk_data.customers.customer_data c
),
us_cities AS (
SELECT
    upper(trim(c.state_abbr)) AS state,
    upper(trim(c.city_name)) AS city,
    geo_location
FROM vk_data.resources.us_cities c
)
SELECT 
    c.customer_name,
    ica.city,
    ica.state,
    apc.food_pref_count,
    round(st_distance(uc.geo_location, chic.geo_location)::number / 1609, 1, 'HALF_TO_EVEN') AS chicago_distance_miles,
    round(st_distance(uc.geo_location, gary.geo_location)::number / 1609, 1, 'HALF_TO_EVEN') AS gary_distance_miles
FROM 
    impacted_customer_address ica
INNER JOIN customers c
    ON ica.customer_id = c.customer_id
INNER JOIN active_pref_customers apc 
    ON ica.customer_id = apc.customer_id
LEFT JOIN us_cities uc
    ON ica.state = uc.state AND ica.city = uc.city
CROSS JOIN 
    chicago_geo_location chic
CROSS JOIN 
     gary_geo_location gary













