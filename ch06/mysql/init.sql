-- Copyright 2026 Spoorthi Basu
-- SPDX-License-Identifier: Apache-2.0

CREATE DATABASE crm;

CREATE TABLE crm.customers (
  customer_id INT PRIMARY KEY,
  plan VARCHAR(32),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
