-- Fix for Supabase Performance Warnings: Multiple Permissive Policies
-- Consolidates RLS policies for public.goals_distribution, public.profiles, and data tables.

-- ==============================================================================
-- Tabela: goals_distribution
-- ==============================================================================
-- Drop conflicting policies from security_fix.sql
DROP POLICY IF EXISTS "Goals Write Admin" ON public.goals_distribution;
DROP POLICY IF EXISTS "Goals Read Approved" ON public.goals_distribution;

-- Ensure the unified policy exists
DROP POLICY IF EXISTS "Acesso Total Unificado" ON public.goals_distribution;

CREATE POLICY "Acesso Total Unificado" ON public.goals_distribution
FOR ALL TO authenticated
USING (public.is_admin() OR public.is_approved())
WITH CHECK (public.is_admin() OR public.is_approved());


-- ==============================================================================
-- Tabela: profiles
-- ==============================================================================
-- Drop conflicting policies
DROP POLICY IF EXISTS "Admin Manage Profiles" ON public.profiles;
DROP POLICY IF EXISTS "Profiles Visibility" ON public.profiles;
DROP POLICY IF EXISTS "Users can view own profile" ON public.profiles;
DROP POLICY IF EXISTS "Users can update own profile" ON public.profiles;

-- Create unified policies for profiles
-- Ensure we drop them first to allow re-running the script
DROP POLICY IF EXISTS "Profiles Unified Select" ON public.profiles;
DROP POLICY IF EXISTS "Profiles Unified Update" ON public.profiles;
DROP POLICY IF EXISTS "Profiles Unified Insert" ON public.profiles;
DROP POLICY IF EXISTS "Profiles Unified Delete" ON public.profiles;

-- 1. SELECT: Users see their own, Admins see all.
CREATE POLICY "Profiles Unified Select" ON public.profiles
FOR SELECT TO authenticated
USING ((select auth.uid()) = id OR public.is_admin());

-- 2. UPDATE: Users update their own, Admins update all.
CREATE POLICY "Profiles Unified Update" ON public.profiles
FOR UPDATE TO authenticated
USING ((select auth.uid()) = id OR public.is_admin())
WITH CHECK ((select auth.uid()) = id OR public.is_admin());

-- 3. INSERT: Admins only (Users created via trigger)
CREATE POLICY "Profiles Unified Insert" ON public.profiles
FOR INSERT TO authenticated
WITH CHECK (public.is_admin());

-- 4. DELETE: Admins only
CREATE POLICY "Profiles Unified Delete" ON public.profiles
FOR DELETE TO authenticated
USING (public.is_admin());


-- ==============================================================================
-- Data Tables Cleanup
-- Drop policies from security_fix.sql that conflict with politicas_unificadas.sql
-- ==============================================================================

DO $$
DECLARE
    t text;
BEGIN
    FOR t IN 
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name IN (
            'data_active_products',
            'data_clients',
            'data_detailed',
            'data_history',
            'data_innovations',
            'data_metadata',
            'data_orders',
            'data_product_details',
            'data_stock'
        )
    LOOP
        EXECUTE format('DROP POLICY IF EXISTS "Read Access Approved" ON public.%I;', t);
        EXECUTE format('DROP POLICY IF EXISTS "Write Access Admin" ON public.%I;', t);
        EXECUTE format('DROP POLICY IF EXISTS "Update Access Admin" ON public.%I;', t);
        EXECUTE format('DROP POLICY IF EXISTS "Delete Access Admin" ON public.%I;', t);
    END LOOP;
END $$;
