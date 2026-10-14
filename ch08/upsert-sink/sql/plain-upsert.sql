-- Copyright 2026 Spoorthi Basu
-- SPDX-License-Identifier: Apache-2.0

-- The update (source version 2) arrives first, then the insert (source version 1)
-- arrives late. A plain upsert applies whatever arrives last.
INSERT INTO customer_docs VALUES (4412, 'enterprise', 2, '[0,1,0]')
ON CONFLICT (customer_id) DO UPDATE
  SET plan = EXCLUDED.plan, source_version = EXCLUDED.source_version, embedding = EXCLUDED.embedding;

INSERT INTO customer_docs VALUES (4412, 'trial', 1, '[1,0,0]')
ON CONFLICT (customer_id) DO UPDATE
  SET plan = EXCLUDED.plan, source_version = EXCLUDED.source_version, embedding = EXCLUDED.embedding;

SELECT customer_id, plan, source_version FROM customer_docs;

-- A nearest-neighbor lookup, as a retrieval step would run it.
SELECT customer_id, plan, source_version FROM customer_docs
ORDER BY embedding <-> '[0,1,0]' LIMIT 1;
