INSERT INTO crm.customers (customer_id, plan) VALUES (4412, 'trial');
ALTER TABLE crm.customers ADD COLUMN region VARCHAR(16);
UPDATE crm.customers SET plan = 'enterprise' WHERE customer_id = 4412;
