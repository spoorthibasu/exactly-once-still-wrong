-- Copyright 2026 Spoorthi Basu
-- SPDX-License-Identifier: Apache-2.0

SELECT customer_id, plan, updated_at, region, _file
FROM lake.crm.customers
ORDER BY updated_at, _file;
