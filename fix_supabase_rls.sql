-- Fix for Supabase RLS Warnings

-- This file contains SQL commands to resolve the warnings reported by the Supabase database linter.
-- Run these commands in the Supabase SQL Editor.

-- =============================================================================
-- 1. Fix "Auth RLS Initialization Plan" for 'profiles' table
-- =============================================================================
-- The original policy re-evaluates auth.uid() for each row.
-- Wrapping it in (select ...) optimizes performance.

DROP POLICY IF EXISTS "Users can view own profile" ON public.profiles;
CREATE POLICY "Users can view own profile" ON public.profiles
    FOR SELECT USING ((select auth.uid()) = id);

DROP POLICY IF EXISTS "Users can update own profile" ON public.profiles;
CREATE POLICY "Users can update own profile" ON public.profiles
    FOR UPDATE USING ((select auth.uid()) = id);

-- =============================================================================
-- 2. Fix "Multiple Permissive Policies" for 'goals_distribution'
-- =============================================================================
-- The warning indicates multiple permissive policies for SELECT.
-- Likely "Enable read access for all users" (public) and "Goals: managers or owner can modify" (restricted).
-- If you intend to restrict access, drop the public policy.

DROP POLICY IF EXISTS "Enable read access for all users" ON public.goals_distribution;

-- =============================================================================
-- 3. Fix "Auth RLS Initialization Plan" for 'goals_distribution'
-- =============================================================================
-- The policy "Goals: managers or owner can modify" also has performance issues.
-- Since the definition is not in the source file, you need to recreate it with the fix.
-- Below is a TEMPLATE. You must replace '...' with your actual logic.

-- DROP POLICY IF EXISTS "Goals: managers or owner can modify" ON public.goals_distribution;
-- CREATE POLICY "Goals: managers or owner can modify" ON public.goals_distribution
--    FOR ALL -- (or SELECT/INSERT/UPDATE)
--    USING (
--        (select auth.uid()) = ... OR (select auth.role()) = '...'
--    );

-- =============================================================================
-- 4. Fix "Auth RLS Initialization Plan" for other tables
-- =============================================================================
-- The following tables have a policy named "Authenticated can read [table]" which causes warnings.
-- We assume these policies check if the user is authenticated.
-- If your policy logic is different (e.g., checks user ID), adjust accordingly.

-- Fix for data_history
DROP POLICY IF EXISTS "Authenticated can read data_history" ON public.data_history;
CREATE POLICY "Authenticated can read data_history" ON public.data_history
    FOR SELECT USING ((select auth.role()) = 'authenticated');

-- Fix for data_clients
DROP POLICY IF EXISTS "Authenticated can read data_clients" ON public.data_clients;
CREATE POLICY "Authenticated can read data_clients" ON public.data_clients
    FOR SELECT USING ((select auth.role()) = 'authenticated');

-- Fix for data_orders
DROP POLICY IF EXISTS "Authenticated can read data_orders" ON public.data_orders;
CREATE POLICY "Authenticated can read data_orders" ON public.data_orders
    FOR SELECT USING ((select auth.role()) = 'authenticated');

-- Fix for data_product_details
DROP POLICY IF EXISTS "Authenticated can read data_product_details" ON public.data_product_details;
CREATE POLICY "Authenticated can read data_product_details" ON public.data_product_details
    FOR SELECT USING ((select auth.role()) = 'authenticated');

-- Fix for data_active_products
DROP POLICY IF EXISTS "Authenticated can read data_active_products" ON public.data_active_products;
CREATE POLICY "Authenticated can read data_active_products" ON public.data_active_products
    FOR SELECT USING ((select auth.role()) = 'authenticated');

-- Fix for data_stock
DROP POLICY IF EXISTS "Authenticated can read data_stock" ON public.data_stock;
CREATE POLICY "Authenticated can read data_stock" ON public.data_stock
    FOR SELECT USING ((select auth.role()) = 'authenticated');

-- Fix for data_innovations
DROP POLICY IF EXISTS "Authenticated can read data_innovations" ON public.data_innovations;
CREATE POLICY "Authenticated can read data_innovations" ON public.data_innovations
    FOR SELECT USING ((select auth.role()) = 'authenticated');

-- Fix for data_metadata
DROP POLICY IF EXISTS "Authenticated can read data_metadata" ON public.data_metadata;
CREATE POLICY "Authenticated can read data_metadata" ON public.data_metadata
    FOR SELECT USING ((select auth.role()) = 'authenticated');
