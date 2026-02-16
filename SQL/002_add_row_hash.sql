
-- Migration: Add row_hash column for Delta Sync
-- Description: Adds a 'row_hash' column to main data tables to enable client-side diffing and incremental uploads.

-- 1. Sales Data
ALTER TABLE data_detailed ADD COLUMN IF NOT EXISTS row_hash TEXT;
CREATE INDEX IF NOT EXISTS idx_data_detailed_row_hash ON data_detailed (row_hash);

-- 2. History Data
ALTER TABLE data_history ADD COLUMN IF NOT EXISTS row_hash TEXT;
CREATE INDEX IF NOT EXISTS idx_data_history_row_hash ON data_history (row_hash);

-- 3. Clients Data
ALTER TABLE data_clients ADD COLUMN IF NOT EXISTS row_hash TEXT;
CREATE INDEX IF NOT EXISTS idx_data_clients_row_hash ON data_clients (row_hash);

-- 4. Stock Data
ALTER TABLE data_stock ADD COLUMN IF NOT EXISTS row_hash TEXT;
CREATE INDEX IF NOT EXISTS idx_data_stock_row_hash ON data_stock (row_hash);

-- 5. Innovations Data
ALTER TABLE data_innovations ADD COLUMN IF NOT EXISTS row_hash TEXT;
CREATE INDEX IF NOT EXISTS idx_data_innovations_row_hash ON data_innovations (row_hash);

-- 6. Active Products (Support Table)
ALTER TABLE data_active_products ADD COLUMN IF NOT EXISTS row_hash TEXT;
CREATE INDEX IF NOT EXISTS idx_data_active_products_row_hash ON data_active_products (row_hash);

-- 7. Product Details (Support Table)
ALTER TABLE data_product_details ADD COLUMN IF NOT EXISTS row_hash TEXT;
CREATE INDEX IF NOT EXISTS idx_data_product_details_row_hash ON data_product_details (row_hash);
