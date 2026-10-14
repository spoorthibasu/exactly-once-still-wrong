SELECT customer_id, count(*) AS live_rows
FROM lake.crm.customers
GROUP BY customer_id
HAVING count(*) > 1;
