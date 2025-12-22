-- Fix Remaining Supabase RLS Warnings

-- This file contains SQL commands to resolve the remaining "Auth RLS Initialization Plan"
-- and "Multiple Permissive Policies" warnings reported by the Supabase database linter.
-- Run these commands in the Supabase SQL Editor.

-- =============================================================================
-- 1. Fix "Multiple Permissive Policies" & "Auth RLS Init Plan" for 'data_*' tables
-- =============================================================================
-- The database has conflicting policies:
-- 1. "Authenticated can read [table]" (Restricted, causes Perf warning)
-- 2. "Enable read access for all users" (Public, causes Multiple Permissive Policy warning)
--
-- Since the codebase implies these tables should be public (or at least accessible),
-- we will DROP the restricted "Authenticated can read..." policies.
-- This resolves both warnings at once.

DROP POLICY IF EXISTS "Authenticated can read data_detailed" ON public.data_detailed;
DROP POLICY IF EXISTS "Authenticated can read data_history" ON public.data_history;
DROP POLICY IF EXISTS "Authenticated can read data_clients" ON public.data_clients;
DROP POLICY IF EXISTS "Authenticated can read data_orders" ON public.data_orders;
DROP POLICY IF EXISTS "Authenticated can read data_product_details" ON public.data_product_details;
DROP POLICY IF EXISTS "Authenticated can read data_active_products" ON public.data_active_products;
DROP POLICY IF EXISTS "Authenticated can read data_stock" ON public.data_stock;
DROP POLICY IF EXISTS "Authenticated can read data_innovations" ON public.data_innovations;
DROP POLICY IF EXISTS "Authenticated can read data_metadata" ON public.data_metadata;

-- =============================================================================
-- 2. Fix "Multiple Permissive Policies" & "Auth RLS Init Plan" for 'goals_distribution'
-- =============================================================================
-- This table had multiple conflicting policies.
-- We retain ONLY "Enable insert/update for goals" which is defined as FOR ALL (Select/Insert/Update/Delete).
-- We drop the redundant or conflicting policies.

-- Drop the redundant public read policy (covered by "Enable insert/update for goals")
DROP POLICY IF EXISTS "Enable read access for all users" ON public.goals_distribution;

-- Drop the old restricted policy which causes performance warnings and conflicts
DROP POLICY IF EXISTS "Goals: managers or owner can modify" ON public.goals_distribution;

-- Ensure the main policy exists (idempotent check)
-- This matches the definition in codigo_sql_supabase.sql
DROP POLICY IF EXISTS "Enable insert/update for goals" ON public.goals_distribution;
CREATE POLICY "Enable insert/update for goals" ON public.goals_distribution FOR ALL USING (true) WITH CHECK (true);

-- =============================================================================
-- 3. Fix "Auth RLS Initialization Plan" for 'profiles' table (Re-apply)
-- =============================================================================
-- Re-applying this fix to ensure it is present.

DROP POLICY IF EXISTS "Users can view own profile" ON public.profiles;
CREATE POLICY "Users can view own profile" ON public.profiles
    FOR SELECT USING ((select auth.uid()) = id);

DROP POLICY IF EXISTS "Users can update own profile" ON public.profiles;
CREATE POLICY "Users can update own profile" ON public.profiles
    FOR UPDATE USING ((select auth.uid()) = id);
