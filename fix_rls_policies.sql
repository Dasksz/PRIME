-- SQL script to remove old and conflicting RLS policies.
-- Execute this script in your Supabase SQL editor to resolve the "Multiple Permissive Policies" warnings.

-- Cleanup for public.profiles table
DROP POLICY IF EXISTS "Enable read access for anon users" ON public.profiles;
DROP POLICY IF EXISTS "Enable read access for authenticated users" ON public.profiles;
DROP POLICY IF EXISTS "Enable read access for everyone" ON public.profiles;
DROP POLICY IF EXISTS "Enable read access for service_role users" ON public.profiles;
DROP POLICY IF EXISTS "Allow authenticated users to read profiles" ON public.profiles; -- Added from V13 script for safety
DROP POLICY IF EXISTS "Allow users to read their own profile" ON public.profiles; -- Added from V13 script for safety


-- Cleanup for public.data_clients table
DROP POLICY IF EXISTS "Enable read access for anon users" ON public.data_clients;
DROP POLICY IF EXISTS "Enable read access for authenticated users" ON public.data_clients;
DROP POLICY IF EXISTS "Enable read access for everyone" ON public.data_clients;
DROP POLICY IF EXISTS "Enable read access for service_role users" ON public.data_clients;
DROP POLICY IF EXISTS "Allow read access to approved users" ON public.data_clients; -- Added from V13 script for safety

-- Cleanup for public.data_detailed table
DROP POLICY IF EXISTS "Enable read access for anon users" ON public.data_detailed;
DROP POLICY IF EXISTS "Enable read access for authenticated users" ON public.data_detailed;
DROP POLICY IF EXISTS "Enable read access for everyone" ON public.data_detailed;
DROP POLICY IF EXISTS "Enable read access for service_role users" ON public.data_detailed;
DROP POLICY IF EXISTS "Allow read access to approved users" ON public.data_detailed; -- Added from V13 script for safety

-- Cleanup for public.data_history table
DROP POLICY IF EXISTS "Enable read access for anon users" ON public.data_history;
DROP POLICY IF EXISTS "Enable read access for authenticated users" ON public.data_history;
DROP POLICY IF EXISTS "Enable read access for everyone" ON public.data_history;
DROP POLICY IF EXISTS "Enable read access for service_role users" ON public.data_history;
DROP POLICY IF EXISTS "Allow read access to approved users" ON public.data_history; -- Added from V13 script for safety

-- Cleanup for public.data_stock table
DROP POLICY IF EXISTS "Enable read access for anon users" ON public.data_stock;
DROP POLICY IF EXISTS "Enable read access for authenticated users" ON public.data_stock;
DROP POLICY IF EXISTS "Enable read access for everyone" ON public.data_stock;
DROP POLICY IF EXISTS "Enable read access for service_role users" ON public.data_stock;
DROP POLICY IF EXISTS "Allow read access to approved users" ON public.data_stock; -- Added from V13 script for safety

-- Cleanup for public.data_active_products table
DROP POLICY IF EXISTS "Enable read access for anon users" ON public.data_active_products;
DROP POLICY IF EXISTS "Enable read access for authenticated users" ON public.data_active_products;
DROP POLICY IF EXISTS "Enable read access for everyone" ON public.data_active_products;
DROP POLICY IF EXISTS "Enable read access for service_role users" ON public.data_active_products;
DROP POLICY IF EXISTS "Allow read access to approved users" ON public.data_active_products; -- Added from V13 script for safety

-- Cleanup for public.data_product_details table
DROP POLICY IF EXISTS "Enable read access for anon users" ON public.data_product_details;
DROP POLICY IF EXISTS "Enable read access for authenticated users" ON public.data_product_details;
DROP POLICY IF EXISTS "Enable read access for everyone" ON public.data_product_details;
DROP POLICY IF EXISTS "Enable read access for service_role users" ON public.data_product_details;
DROP POLICY IF EXISTS "Allow read access to approved users" ON public.data_product_details; -- Added from V13 script for safety

-- Cleanup for public.data_metadata table
DROP POLICY IF EXISTS "Enable read access for anon users" ON public.data_metadata;
DROP POLICY IF EXISTS "Enable read access for authenticated users" ON public.data_metadata;
DROP POLICY IF EXISTS "Enable read access for everyone" ON public.data_metadata;
DROP POLICY IF EXISTS "Enable read access for service_role users" ON public.data_metadata;
DROP POLICY IF EXISTS "Allow read access to approved users" ON public.data_metadata; -- Added from V13 script for safety

-- Note: This script uses "DROP POLICY IF EXISTS" to prevent errors if a policy has already been removed.

-- =================================================================
-- PERFORMANCE INDEX
-- Creates an index on the profiles.id column to speed up user lookups.
-- =================================================================

-- Note: The Supabase SQL editor runs queries in a transaction.
-- `CREATE INDEX CONCURRENTLY` cannot run inside a transaction.
-- If this command fails, please remove the "CONCURRENTLY" keyword and run it again.
create index concurrently IF NOT exists
  profiles_id_idx on public.profiles (id);
