-- Copyright 2026 Spoorthi Basu
-- SPDX-License-Identifier: Apache-2.0

-- What an agent's retrieval reads: one row per customer, with an embedding.
CREATE EXTENSION IF NOT EXISTS vector;
DROP TABLE IF EXISTS customer_docs;
CREATE TABLE customer_docs (
  customer_id    int PRIMARY KEY,
  plan           text NOT NULL,
  source_version int  NOT NULL,
  embedding      vector(3) NOT NULL
);
