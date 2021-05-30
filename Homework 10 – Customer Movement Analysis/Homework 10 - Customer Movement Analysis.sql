with customer as 
(
select distinct cust_code ,
DATE_TRUNC(PARSE_DATE("%Y%m%d", CAST(SHOP_DATE AS STRING)) , Month) as current_month ,
from `hw6-big-query.supermarket_1.data001`
),
customer_month as 
(
select cust_code, current_month, LAG(current_month,1) OVER (PARTITION BY cust_code ORDER BY current_month) AS previous_month 
FROM customer
),
Result_customer1 as (
SELECT cust_code, current_month , previous_month,
CASE WHEN DATE_DIFF (current_month, previous_month, MONTH) = 1  THEN 'Repeat_customer' 
    WHEN DATE_DIFF(current_month, previous_month, MONTH) > 1 THEN 'Reactivated_customer'
    WHEN DATE_DIFF(current_month, previous_month, MONTH) IS NULL THEN 'New_customer' End as Status
FROM customer_month
),
Result_customer2 as (
select cust_code ,current_month ,DATE_ADD(current_month, INTERVAL 1 MONTH) as Future_month ,
case when current_month <= (select MAX(current_month) from customer) then 'Churn_customer' end as status 
from (
select cust_code , current_month ,ROW_NUMBER() OVER ( PARTITION BY cust_code ORDER BY current_month DESC ) as rwn
from customer) t where rwn = 1)


select current_month ,status ,count(Cust_code) from(
select cust_code , current_month , status 
from Result_customer1

UNION ALL 
select cust_code , Future_month , status
from Result_customer2 where Future_month <= (select MAX(current_month) from customer) )

group by current_month ,status 