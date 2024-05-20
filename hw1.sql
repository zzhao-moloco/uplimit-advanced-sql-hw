-- EXERCISE 1

-- My approach is to verify  and then follow the steps to create the final query.
-- Step 1: We have 10,000 potential customers who have signed up with Virtual Kitchen. If the customer is able to order from us, then their city/state will be present in our database. Create a query in Snowflake that returns all customers that can place an order with Virtual Kitchen.

-- All customer_ids exist in vk_data.customers.customer_address
-- select 
--     cd.customer_id
-- from vk_data.customers.customer_data cd
-- left join vk_data.customers.customer_address ca
--     on cd.customer_id = ca.customer_id
-- where ca.customer_id is null;

-- The cities and state names are both string-like, one is varchar(50), one is varchar(150)
-- describe table vk_data.customers.customer_address; 
-- describe table vk_data.resources.us_cities;

-- No null city or state names
-- select * from vk_data.customers.customer_address
-- where customer_city is null or customer_state is null;

-- select * from vk_data.resources.us_cities
-- where city_name is null or state_abbr is null;

-- Check that each customer only have 1 address
-- select 
--     customer_id, 
--     count(address_id)
-- from vk_data.customers.customer_address
-- group by customer_id
-- having count(address_id) > 1;


-- Trim and uppercase the city and state names to align in the two tables 
-- select address_id, upper(trim(customer_city)) as city, upper(trim(customer_state)) as state
-- from vk_data.customers.customer_address 
-- limit 10;

-- select city_id, upper(trim(city_name)) as city, upper(trim(state_abbr)) as state
-- from vk_data.resources.us_cities
-- limit 10;

-- Query for step 1
with sanitized_customer_address as (
select
    ca.address_id,
    ca.customer_id,
    upper(trim(ca.customer_city)) as customer_city,
    upper(trim(ca.customer_state)) as customer_state,
from vk_data.customers.customer_address ca
left join vk_data.resources.us_cities uc
on upper(trim(ca.customer_city)) = upper(trim(uc.city_name))
    and upper(trim(ca.customer_state)) = upper(trim(uc.state_abbr))
)
select
    cd.customer_id,
    first_name,
    last_name,
    email,
    customer_city,
    customer_state
from 
    vk_data.customers.customer_data cd
left join sanitized_customer_address sca
    on cd.customer_id = sca.customer_id;


-- Step 2: We have 10 suppliers in the United States. Each customer should be fulfilled by the closest distribution center. Determine which supplier is closest to each customer, and how far the shipment needs to travel to reach the customer. There are a few different ways to complete this step. Use the customer's city and state to join to the us_cities resource table. Do not worry about zip code for this exercise. Order your results by the customer's last name and first name.

-- Suppliers all have city and state
-- select *
-- from vk_data.suppliers.supplier_info
-- where supplier_city is null or supplier_state is null;

-- Match suppliers with us_cities
-- select
--    supplier_id,
--    supplier_name,
--    upper(trim(si.supplier_city)) as supplier_city,
--    upper(trim(si.supplier_state)) as supplier_state,
--    geo_location
-- from vk_data.suppliers.supplier_info si
-- left join vk_data.resources.us_cities uc
-- on upper(trim(si.supplier_city)) = upper(trim(uc.city_name))
--     and upper(trim(si.supplier_state)) = upper(trim(uc.state_abbr));

-- Put step 1 and step 2 together with cross join and row number on distance
with customer_us_cities as (
select
    ca.customer_id,
    geo_location as customer_geo_location
from vk_data.customers.customer_address ca
left join vk_data.resources.us_cities uc
on upper(trim(ca.customer_city)) = upper(trim(uc.city_name))
    and upper(trim(ca.customer_state)) = upper(trim(uc.state_abbr))
where uc.city_id is not null
),
full_customer as (
select
    cd.customer_id,
    first_name,
    last_name,
    email,
    customer_geo_location
from 
    vk_data.customers.customer_data cd
left join customer_us_cities cuc
    on cd.customer_id = cuc.customer_id
where cuc.customer_id is not null
),
full_supplier as (
select
    supplier_id,
    supplier_name,
    geo_location as supplier_geo_location
from vk_data.suppliers.supplier_info si
left join vk_data.resources.us_cities uc
on upper(trim(si.supplier_city)) = upper(trim(uc.city_name))
    and upper(trim(si.supplier_state)) = upper(trim(uc.state_abbr))
where uc.city_id is not null
),
customer_supplier_distances as (
select
    fc.*,
    fs.*,
    st_distance(fc.customer_geo_location, fs.supplier_geo_location) as distance_meters,
    row_number() over (partition by fc.customer_id order by distance_meters asc) as row_number_distance_meters
from full_customer fc
cross join full_supplier fs
)
select
    customer_id,
    first_name,
    last_name,
    email,
    supplier_id,
    supplier_name,
    distance_meters
from customer_supplier_distances d
where row_number_distance_meters = 1
order by last_name, first_name

--- EXERCISE 2
--- to do

