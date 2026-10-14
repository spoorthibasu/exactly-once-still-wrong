-- Copyright 2026 Spoorthi Basu
-- SPDX-License-Identifier: Apache-2.0

-- The same arrival order, with a version check on write: the update applies only
-- when the incoming source version is newer than the stored one.
INSERT INTO customer_docs VALUES (4412, 'enterprise', 2, '[0,1,0]')
ON CONFLICT (customer_id) DO UPDATE
  SET plan = EXCLUDED.plan, source_version = EXCLUDED.source_version, embedding = EXCLUDED.embedding
  WHERE customer_docs.source_version < EXCLUDED.source_version;

INSERT INTO customer_docs VALUES (4412, 'trial', 1, '[1,0,0]')
ON CONFLICT (customer_id) DO UPDATE
  SET plan = EXCLUDED.plan, source_version = EXCLUDED.source_version, embedding = EXCLUDED.embedding
  WHERE customer_docs.source_version < EXCLUDED.source_version;

SELECT customer_id, plan, source_version FROM customer_docs;

SELECT customer_id, plan, source_version FROM customer_docs
ORDER BY embedding <-> '[0,1,0]' LIMIT 1;
