-- Correção de Avisos de Performance e Segurança do Supabase
-- Versão Idempotente: Pode ser rodada múltiplas vezes sem erro.

-- 1. Remover índices duplicados
DROP INDEX IF EXISTS public.idx_detailed_codusur;
DROP INDEX IF EXISTS public.idx_history_codusur;

-- 2. Função auxiliar para verificar admin
CREATE OR REPLACE FUNCTION public.is_admin()
RETURNS boolean AS $$
BEGIN
  IF (select auth.role()) = 'service_role' THEN RETURN true; END IF;
  RETURN EXISTS (SELECT 1 FROM public.profiles WHERE id = (select auth.uid()) AND role = 'admin');
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Função auxiliar para verificar aprovado
CREATE OR REPLACE FUNCTION public.is_approved()
RETURNS boolean AS $$
BEGIN
  IF (select auth.role()) = 'service_role' THEN RETURN true; END IF;
  RETURN EXISTS (
    SELECT 1 FROM public.profiles
    WHERE id = (select auth.uid())
    AND status = 'aprovado'
  );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


-- ==============================================================================
-- Tabela: data_detailed
-- ==============================================================================
-- Dropar políticas antigas
DROP POLICY IF EXISTS "Acesso escrita admin" ON public.data_detailed;
DROP POLICY IF EXISTS "Acesso leitura seguro" ON public.data_detailed;
DROP POLICY IF EXISTS "Acesso leitura aprovados" ON public.data_detailed;
DROP POLICY IF EXISTS "Enable read access for all users" ON public.data_detailed;

-- Dropar novas políticas (para garantir idempotência)
DROP POLICY IF EXISTS "Acesso Leitura Unificado" ON public.data_detailed;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Insert)" ON public.data_detailed;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Update)" ON public.data_detailed;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Delete)" ON public.data_detailed;

CREATE POLICY "Acesso Leitura Unificado" ON public.data_detailed
FOR SELECT TO authenticated
USING (public.is_admin() OR public.is_approved());

CREATE POLICY "Acesso Escrita Admin (Insert)" ON public.data_detailed
FOR INSERT TO authenticated
WITH CHECK (public.is_admin());

CREATE POLICY "Acesso Escrita Admin (Update)" ON public.data_detailed
FOR UPDATE TO authenticated
USING (public.is_admin())
WITH CHECK (public.is_admin());

CREATE POLICY "Acesso Escrita Admin (Delete)" ON public.data_detailed
FOR DELETE TO authenticated
USING (public.is_admin());


-- ==============================================================================
-- Tabela: data_history
-- ==============================================================================
DROP POLICY IF EXISTS "Acesso escrita admin" ON public.data_history;
DROP POLICY IF EXISTS "Acesso leitura seguro" ON public.data_history;
DROP POLICY IF EXISTS "Acesso leitura aprovados" ON public.data_history;
DROP POLICY IF EXISTS "Enable read access for all users" ON public.data_history;

DROP POLICY IF EXISTS "Acesso Leitura Unificado" ON public.data_history;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Insert)" ON public.data_history;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Update)" ON public.data_history;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Delete)" ON public.data_history;

CREATE POLICY "Acesso Leitura Unificado" ON public.data_history
FOR SELECT TO authenticated
USING (public.is_admin() OR public.is_approved());

CREATE POLICY "Acesso Escrita Admin (Insert)" ON public.data_history
FOR INSERT TO authenticated
WITH CHECK (public.is_admin());

CREATE POLICY "Acesso Escrita Admin (Update)" ON public.data_history
FOR UPDATE TO authenticated
USING (public.is_admin())
WITH CHECK (public.is_admin());

CREATE POLICY "Acesso Escrita Admin (Delete)" ON public.data_history
FOR DELETE TO authenticated
USING (public.is_admin());


-- ==============================================================================
-- Tabela: data_clients
-- ==============================================================================
DROP POLICY IF EXISTS "Acesso escrita admin" ON public.data_clients;
DROP POLICY IF EXISTS "Acesso leitura seguro" ON public.data_clients;
DROP POLICY IF EXISTS "Acesso leitura aprovados" ON public.data_clients;
DROP POLICY IF EXISTS "Enable read access for all users" ON public.data_clients;

DROP POLICY IF EXISTS "Acesso Leitura Unificado" ON public.data_clients;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Insert)" ON public.data_clients;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Update)" ON public.data_clients;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Delete)" ON public.data_clients;

CREATE POLICY "Acesso Leitura Unificado" ON public.data_clients
FOR SELECT TO authenticated
USING (public.is_admin() OR public.is_approved());

CREATE POLICY "Acesso Escrita Admin (Insert)" ON public.data_clients
FOR INSERT TO authenticated
WITH CHECK (public.is_admin());

CREATE POLICY "Acesso Escrita Admin (Update)" ON public.data_clients
FOR UPDATE TO authenticated
USING (public.is_admin())
WITH CHECK (public.is_admin());

CREATE POLICY "Acesso Escrita Admin (Delete)" ON public.data_clients
FOR DELETE TO authenticated
USING (public.is_admin());


-- ==============================================================================
-- Tabela: data_orders
-- ==============================================================================
DROP POLICY IF EXISTS "Acesso escrita admin" ON public.data_orders;
DROP POLICY IF EXISTS "Acesso leitura seguro" ON public.data_orders;
DROP POLICY IF EXISTS "Acesso leitura aprovados" ON public.data_orders;
DROP POLICY IF EXISTS "Enable read access for all users" ON public.data_orders;

DROP POLICY IF EXISTS "Acesso Leitura Unificado" ON public.data_orders;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Insert)" ON public.data_orders;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Update)" ON public.data_orders;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Delete)" ON public.data_orders;

CREATE POLICY "Acesso Leitura Unificado" ON public.data_orders
FOR SELECT TO authenticated
USING (public.is_admin() OR public.is_approved());

CREATE POLICY "Acesso Escrita Admin (Insert)" ON public.data_orders
FOR INSERT TO authenticated
WITH CHECK (public.is_admin());

CREATE POLICY "Acesso Escrita Admin (Update)" ON public.data_orders
FOR UPDATE TO authenticated
USING (public.is_admin())
WITH CHECK (public.is_admin());

CREATE POLICY "Acesso Escrita Admin (Delete)" ON public.data_orders
FOR DELETE TO authenticated
USING (public.is_admin());


-- ==============================================================================
-- Tabela: data_product_details
-- ==============================================================================
DROP POLICY IF EXISTS "Acesso escrita admin" ON public.data_product_details;
DROP POLICY IF EXISTS "Acesso leitura seguro" ON public.data_product_details;
DROP POLICY IF EXISTS "Acesso leitura aprovados" ON public.data_product_details;
DROP POLICY IF EXISTS "Enable read access for all users" ON public.data_product_details;

DROP POLICY IF EXISTS "Acesso Leitura Unificado" ON public.data_product_details;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Insert)" ON public.data_product_details;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Update)" ON public.data_product_details;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Delete)" ON public.data_product_details;

CREATE POLICY "Acesso Leitura Unificado" ON public.data_product_details
FOR SELECT TO authenticated
USING (public.is_admin() OR public.is_approved());

CREATE POLICY "Acesso Escrita Admin (Insert)" ON public.data_product_details
FOR INSERT TO authenticated
WITH CHECK (public.is_admin());

CREATE POLICY "Acesso Escrita Admin (Update)" ON public.data_product_details
FOR UPDATE TO authenticated
USING (public.is_admin())
WITH CHECK (public.is_admin());

CREATE POLICY "Acesso Escrita Admin (Delete)" ON public.data_product_details
FOR DELETE TO authenticated
USING (public.is_admin());


-- ==============================================================================
-- Tabela: data_active_products
-- ==============================================================================
DROP POLICY IF EXISTS "Acesso escrita admin" ON public.data_active_products;
DROP POLICY IF EXISTS "Acesso leitura seguro" ON public.data_active_products;
DROP POLICY IF EXISTS "Acesso leitura aprovados" ON public.data_active_products;
DROP POLICY IF EXISTS "Enable read access for all users" ON public.data_active_products;

DROP POLICY IF EXISTS "Acesso Leitura Unificado" ON public.data_active_products;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Insert)" ON public.data_active_products;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Update)" ON public.data_active_products;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Delete)" ON public.data_active_products;

CREATE POLICY "Acesso Leitura Unificado" ON public.data_active_products
FOR SELECT TO authenticated
USING (public.is_admin() OR public.is_approved());

CREATE POLICY "Acesso Escrita Admin (Insert)" ON public.data_active_products
FOR INSERT TO authenticated
WITH CHECK (public.is_admin());

CREATE POLICY "Acesso Escrita Admin (Update)" ON public.data_active_products
FOR UPDATE TO authenticated
USING (public.is_admin())
WITH CHECK (public.is_admin());

CREATE POLICY "Acesso Escrita Admin (Delete)" ON public.data_active_products
FOR DELETE TO authenticated
USING (public.is_admin());


-- ==============================================================================
-- Tabela: data_stock
-- ==============================================================================
DROP POLICY IF EXISTS "Acesso escrita admin" ON public.data_stock;
DROP POLICY IF EXISTS "Acesso leitura seguro" ON public.data_stock;
DROP POLICY IF EXISTS "Acesso leitura aprovados" ON public.data_stock;
DROP POLICY IF EXISTS "Enable read access for all users" ON public.data_stock;

DROP POLICY IF EXISTS "Acesso Leitura Unificado" ON public.data_stock;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Insert)" ON public.data_stock;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Update)" ON public.data_stock;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Delete)" ON public.data_stock;

CREATE POLICY "Acesso Leitura Unificado" ON public.data_stock
FOR SELECT TO authenticated
USING (public.is_admin() OR public.is_approved());

CREATE POLICY "Acesso Escrita Admin (Insert)" ON public.data_stock
FOR INSERT TO authenticated
WITH CHECK (public.is_admin());

CREATE POLICY "Acesso Escrita Admin (Update)" ON public.data_stock
FOR UPDATE TO authenticated
USING (public.is_admin())
WITH CHECK (public.is_admin());

CREATE POLICY "Acesso Escrita Admin (Delete)" ON public.data_stock
FOR DELETE TO authenticated
USING (public.is_admin());


-- ==============================================================================
-- Tabela: data_innovations
-- ==============================================================================
DROP POLICY IF EXISTS "Acesso escrita admin" ON public.data_innovations;
DROP POLICY IF EXISTS "Acesso leitura seguro" ON public.data_innovations;
DROP POLICY IF EXISTS "Acesso leitura aprovados" ON public.data_innovations;
DROP POLICY IF EXISTS "Enable read access for all users" ON public.data_innovations;

DROP POLICY IF EXISTS "Acesso Leitura Unificado" ON public.data_innovations;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Insert)" ON public.data_innovations;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Update)" ON public.data_innovations;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Delete)" ON public.data_innovations;

CREATE POLICY "Acesso Leitura Unificado" ON public.data_innovations
FOR SELECT TO authenticated
USING (public.is_admin() OR public.is_approved());

CREATE POLICY "Acesso Escrita Admin (Insert)" ON public.data_innovations
FOR INSERT TO authenticated
WITH CHECK (public.is_admin());

CREATE POLICY "Acesso Escrita Admin (Update)" ON public.data_innovations
FOR UPDATE TO authenticated
USING (public.is_admin())
WITH CHECK (public.is_admin());

CREATE POLICY "Acesso Escrita Admin (Delete)" ON public.data_innovations
FOR DELETE TO authenticated
USING (public.is_admin());


-- ==============================================================================
-- Tabela: data_metadata
-- ==============================================================================
DROP POLICY IF EXISTS "Acesso escrita admin" ON public.data_metadata;
DROP POLICY IF EXISTS "Acesso leitura seguro" ON public.data_metadata;
DROP POLICY IF EXISTS "Acesso leitura aprovados" ON public.data_metadata;
DROP POLICY IF EXISTS "Enable read access for all users" ON public.data_metadata;

DROP POLICY IF EXISTS "Acesso Leitura Unificado" ON public.data_metadata;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Insert)" ON public.data_metadata;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Update)" ON public.data_metadata;
DROP POLICY IF EXISTS "Acesso Escrita Admin (Delete)" ON public.data_metadata;

CREATE POLICY "Acesso Leitura Unificado" ON public.data_metadata
FOR SELECT TO authenticated
USING (public.is_admin() OR public.is_approved());

CREATE POLICY "Acesso Escrita Admin (Insert)" ON public.data_metadata
FOR INSERT TO authenticated
WITH CHECK (public.is_admin());

CREATE POLICY "Acesso Escrita Admin (Update)" ON public.data_metadata
FOR UPDATE TO authenticated
USING (public.is_admin())
WITH CHECK (public.is_admin());

CREATE POLICY "Acesso Escrita Admin (Delete)" ON public.data_metadata
FOR DELETE TO authenticated
USING (public.is_admin());


-- ==============================================================================
-- Tabela: goals_distribution
-- ==============================================================================
DROP POLICY IF EXISTS "Acesso escrita admin" ON public.goals_distribution;
DROP POLICY IF EXISTS "Acesso leitura seguro" ON public.goals_distribution;
DROP POLICY IF EXISTS "Acesso leitura aprovados" ON public.goals_distribution;
DROP POLICY IF EXISTS "Acesso escrita aprovados" ON public.goals_distribution;
DROP POLICY IF EXISTS "Enable read access for all users" ON public.goals_distribution;
DROP POLICY IF EXISTS "Enable insert/update for goals" ON public.goals_distribution;
DROP POLICY IF EXISTS "Acesso Total Aprovados" ON public.goals_distribution;
DROP POLICY IF EXISTS "Acesso Total Aprovados e Admin" ON public.goals_distribution;

-- Drop políticas conflitantes de security_fix.sql
DROP POLICY IF EXISTS "Goals Write Admin" ON public.goals_distribution;
DROP POLICY IF EXISTS "Goals Read Approved" ON public.goals_distribution;

DROP POLICY IF EXISTS "Acesso Total Unificado" ON public.goals_distribution;

CREATE POLICY "Acesso Total Unificado" ON public.goals_distribution
FOR ALL TO authenticated
USING (public.is_admin() OR public.is_approved())
WITH CHECK (public.is_admin() OR public.is_approved());


-- ==============================================================================
-- Tabela: profiles
-- ==============================================================================
-- Drop políticas antigas e conflitantes
DROP POLICY IF EXISTS "Admin Manage Profiles" ON public.profiles;
DROP POLICY IF EXISTS "Profiles Visibility" ON public.profiles;
DROP POLICY IF EXISTS "Users can view own profile" ON public.profiles;
DROP POLICY IF EXISTS "Users can update own profile" ON public.profiles;
DROP POLICY IF EXISTS "Profiles Unified Select" ON public.profiles;
DROP POLICY IF EXISTS "Profiles Unified Update" ON public.profiles;
DROP POLICY IF EXISTS "Profiles Unified Insert" ON public.profiles;
DROP POLICY IF EXISTS "Profiles Unified Delete" ON public.profiles;

-- Create unified policies for profiles
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
