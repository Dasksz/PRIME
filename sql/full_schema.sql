-- ==============================================================================
-- UNIFIED SQL SCHEMA
-- This file unifies all previous SQL scripts into a single, idempotent schema definition.
-- It includes table definitions, RLS policies, helper functions, and security fixes.
--
-- INSTRUCTIONS FOR ADMIN SETUP:
-- To make a user an admin, run the following SQL (replace with the user's email):
-- UPDATE public.profiles SET role = 'adm', status = 'aprovado' WHERE email = 'user@example.com';
-- ==============================================================================

-- Enable UUID extension
create extension if not exists "uuid-ossp";

-- ==============================================================================
-- 1. TABLES
-- ==============================================================================

-- 1.1 Tabela de Vendas Detalhadas (Mês Atual)
create table if not exists public.data_detailed (
  id uuid default uuid_generate_v4 () primary key,
  pedido text,
  nome text, -- Vendedor
  superv text, -- Supervisor
  produto text,
  descricao text,
  fornecedor text,
  observacaofor text, -- Pasta
  codfor text,
  codusur text,
  codcli text,
  qtvenda numeric,
  codsupervisor text,
  vlvenda numeric,
  vlbonific numeric,
  totpesoliq numeric,
  dtped timestamp with time zone,
  dtsaida timestamp with time zone,
  posicao text,
  estoqueunit numeric,
  qtvenda_embalagem_master numeric,
  tipovenda text,
  filial text,
  cliente_nome text, -- Otimização
  cidade text,
  bairro text
);

-- 1.2 Tabela de Histórico de Vendas (Trimestre)
create table if not exists public.data_history (
  id uuid default uuid_generate_v4 () primary key,
  pedido text,
  nome text,
  superv text,
  produto text,
  descricao text,
  fornecedor text,
  observacaofor text,
  codfor text,
  codusur text,
  codcli text,
  qtvenda numeric,
  codsupervisor text,
  vlvenda numeric,
  vlbonific numeric,
  totpesoliq numeric,
  dtped timestamp with time zone,
  dtsaida timestamp with time zone,
  posicao text,
  estoqueunit numeric,
  qtvenda_embalagem_master numeric,
  tipovenda text,
  filial text
);

-- 1.3 Tabela de Clientes
create table if not exists public.data_clients (
  id uuid default uuid_generate_v4 () primary key,
  codigo_cliente text unique,
  rca1 text,
  rca2 text,
  rcas text[], -- Array de RCAs
  cidade text,
  nomecliente text,
  bairro text,
  razaosocial text,
  fantasia text,
  cnpj_cpf text,
  endereco text,
  numero text,
  cep text,
  telefone text,
  email text,
  ramo text,
  ultimacompra timestamp with time zone,
  datacadastro timestamp with time zone,
  bloqueio text,
  inscricaoestadual text
);

-- 1.4 Tabela de Pedidos Agregados
create table if not exists public.data_orders (
  id uuid default uuid_generate_v4 () primary key,
  pedido text unique,
  codcli text,
  cliente_nome text,
  cidade text,
  nome text, -- Vendedor
  superv text, -- Supervisor
  fornecedores_str text,
  dtped timestamp with time zone,
  dtsaida timestamp with time zone,
  posicao text,
  vlvenda numeric,
  totpesoliq numeric,
  filial text,
  tipovenda text,
  fornecedores_list text[],
  codfors_list text[]
);

-- 1.5 Tabela de Detalhes de Produtos
create table if not exists public.data_product_details (
  code text primary key,
  descricao text,
  fornecedor text,
  codfor text,
  dtcadastro timestamp with time zone,
  pasta text
);

-- 1.6 Tabela de Produtos Ativos
create table if not exists public.data_active_products (code text primary key);

-- 1.7 Tabela de Estoque
create table if not exists public.data_stock (
  id uuid default uuid_generate_v4 () primary key,
  product_code text,
  filial text,
  stock_qty numeric
);

-- 1.8 Tabela de Inovações
create table if not exists public.data_innovations (
  id uuid default uuid_generate_v4 () primary key,
  codigo text,
  produto text,
  inovacoes text
);

-- 1.9 Tabela de Metadados
create table if not exists public.data_metadata (key text primary key, value text);

-- 1.10 Tabela para Salvar Metas
create table if not exists public.goals_distribution (
  id uuid default uuid_generate_v4 () primary key,
  month_key text not null,
  supplier text not null,
  brand text default 'GENERAL',
  goals_data jsonb not null,
  updated_at timestamp with time zone default now(),
  updated_by text
);

create unique index if not exists idx_goals_unique on public.goals_distribution (month_key, supplier, brand);

-- 1.11 Tabela de Perfis de Usuário
create table if not exists public.profiles (
  id uuid references auth.users on delete cascade not null primary key,
  email text,
  status text default 'pendente', -- pendente, aprovado, bloqueado
  role text default 'user',
  created_at timestamp with time zone default now(),
  updated_at timestamp with time zone default now()
);

-- 1.12 Tabela de Coordenadas de Clientes
create table if not exists public.data_client_coordinates (
    client_code text primary key,
    lat double precision not null,
    lng double precision not null,
    address text,
    updated_at timestamp with time zone default now()
);

-- Ensure columns exist (Idempotency for older schemas)
DO $$
BEGIN
    ALTER TABLE public.data_detailed ADD COLUMN IF NOT EXISTS observacaofor text;
    ALTER TABLE public.data_history ADD COLUMN IF NOT EXISTS observacaofor text;
    ALTER TABLE public.data_orders ADD COLUMN IF NOT EXISTS tipovenda text;
    ALTER TABLE public.data_orders ADD COLUMN IF NOT EXISTS fornecedores_list text[];
    ALTER TABLE public.data_orders ADD COLUMN IF NOT EXISTS codfors_list text[];
    ALTER TABLE public.data_product_details ADD COLUMN IF NOT EXISTS pasta text;
END $$;


-- ==============================================================================
-- 2. HELPER FUNCTIONS
-- ==============================================================================

-- 2.1 Check is_admin
CREATE OR REPLACE FUNCTION public.is_admin()
RETURNS boolean AS $$
BEGIN
  -- Service Role always admin
  IF (select auth.role()) = 'service_role' THEN RETURN true; END IF;

  -- Check profiles table for 'adm' role
  RETURN EXISTS (
    SELECT 1 FROM public.profiles
    WHERE id = (select auth.uid())
    AND role = 'adm'
  );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = public;

-- 2.2 Check is_approved
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
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = public;

-- 2.3 Truncate Table (Secure)
CREATE OR REPLACE FUNCTION public.truncate_table(table_name text)
RETURNS void AS $$
BEGIN
  -- Security check
  IF NOT public.is_admin() THEN
    RAISE EXCEPTION 'Access denied. Only admins can truncate tables.';
  END IF;

  -- Whitelist validation
  IF table_name NOT IN (
    'data_detailed', 'data_history', 'data_clients', 'data_orders',
    'data_product_details', 'data_active_products', 'data_stock',
    'data_innovations', 'data_metadata', 'goals_distribution'
  ) THEN
    RAISE EXCEPTION 'Invalid table name.';
  END IF;

  EXECUTE format('TRUNCATE TABLE public.%I;', table_name);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = public;

-- 2.4 Get Initial Dashboard Data (Stub)
CREATE OR REPLACE FUNCTION public.get_initial_dashboard_data()
RETURNS json AS $$
BEGIN
  return '{}'::json;
end;
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = public;


-- ==============================================================================
-- 3. TRIGGERS
-- ==============================================================================

-- 3.1 Handle New User (Create Profile)
CREATE OR REPLACE FUNCTION public.handle_new_user ()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  insert into public.profiles (id, email, status)
  values (new.id, new.email, 'pendente');
  return new;
end;
$$;

DROP TRIGGER IF EXISTS on_auth_user_created ON auth.users;
CREATE TRIGGER on_auth_user_created
AFTER INSERT ON auth.users FOR EACH ROW
EXECUTE PROCEDURE public.handle_new_user ();


-- ==============================================================================
-- 4. ROW LEVEL SECURITY (RLS) & POLICIES
-- ==============================================================================

-- Enable RLS on all tables
DO $$
DECLARE
    t text;
BEGIN
    FOR t IN
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
        AND (table_name LIKE 'data_%' OR table_name = 'goals_distribution' OR table_name = 'profiles')
    LOOP
        EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY;', t);

        -- Revoke public permissions for security
        EXECUTE format('REVOKE ALL ON public.%I FROM anon;', t);
        EXECUTE format('REVOKE ALL ON public.%I FROM authenticated;', t);

        -- Grant minimal permissions to authenticated (RLS will handle access)
        EXECUTE format('GRANT SELECT ON public.%I TO authenticated;', t);
        EXECUTE format('GRANT INSERT, UPDATE, DELETE ON public.%I TO authenticated;', t);
    END LOOP;
END $$;

-- 4.1 Profiles Policies
-- Cleanup old policies
DROP POLICY IF EXISTS "Profiles Visibility" ON public.profiles;
DROP POLICY IF EXISTS "Admin Manage Profiles" ON public.profiles;
DROP POLICY IF EXISTS "Users can view own profile" ON public.profiles;
DROP POLICY IF EXISTS "Users can update own profile" ON public.profiles;
DROP POLICY IF EXISTS "Profiles Unified Select" ON public.profiles;
DROP POLICY IF EXISTS "Profiles Unified Update" ON public.profiles;
DROP POLICY IF EXISTS "Profiles Unified Insert" ON public.profiles;
DROP POLICY IF EXISTS "Profiles Unified Delete" ON public.profiles;

-- Create Unified Policies
-- Select: Users see own, Admins see all
CREATE POLICY "Profiles Unified Select" ON public.profiles
FOR SELECT TO authenticated
USING ((select auth.uid()) = id OR public.is_admin());

-- Update: Users update own, Admins update all
CREATE POLICY "Profiles Unified Update" ON public.profiles
FOR UPDATE TO authenticated
USING ((select auth.uid()) = id OR public.is_admin())
WITH CHECK ((select auth.uid()) = id OR public.is_admin());

-- Insert: Admins only (Users created via trigger)
CREATE POLICY "Profiles Unified Insert" ON public.profiles
FOR INSERT TO authenticated
WITH CHECK (public.is_admin());

-- Delete: Admins only
CREATE POLICY "Profiles Unified Delete" ON public.profiles
FOR DELETE TO authenticated
USING (public.is_admin());


-- 4.2 Data Tables Policies (Standardized: Read=Approved|Admin, Write=Admin)
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
            'data_client_coordinates', -- Applying standardized security
            'data_detailed',
            'data_history',
            'data_innovations',
            'data_metadata',
            'data_orders',
            'data_product_details',
            'data_stock'
        )
    LOOP
        -- Cleanup
        EXECUTE format('DROP POLICY IF EXISTS "Read Access Approved" ON public.%I;', t);
        EXECUTE format('DROP POLICY IF EXISTS "Write Access Admin" ON public.%I;', t);
        EXECUTE format('DROP POLICY IF EXISTS "Update Access Admin" ON public.%I;', t);
        EXECUTE format('DROP POLICY IF EXISTS "Delete Access Admin" ON public.%I;', t);
        EXECUTE format('DROP POLICY IF EXISTS "Acesso Leitura Unificado" ON public.%I;', t);
        EXECUTE format('DROP POLICY IF EXISTS "Acesso Escrita Admin (Insert)" ON public.%I;', t);
        EXECUTE format('DROP POLICY IF EXISTS "Acesso Escrita Admin (Update)" ON public.%I;', t);
        EXECUTE format('DROP POLICY IF EXISTS "Acesso Escrita Admin (Delete)" ON public.%I;', t);
        -- Old/Legacy names
        EXECUTE format('DROP POLICY IF EXISTS "Enable read access for all users" ON public.%I;', t);
        EXECUTE format('DROP POLICY IF EXISTS "Enable insert/update for authenticated users" ON public.%I;', t);
        EXECUTE format('DROP POLICY IF EXISTS "Acesso leitura aprovados" ON public.%I;', t);

        -- Create New Standardized Policies
        EXECUTE format('CREATE POLICY "Acesso Leitura Unificado" ON public.%I FOR SELECT TO authenticated USING (public.is_admin() OR public.is_approved());', t);
        EXECUTE format('CREATE POLICY "Acesso Escrita Admin (Insert)" ON public.%I FOR INSERT TO authenticated WITH CHECK (public.is_admin());', t);
        EXECUTE format('CREATE POLICY "Acesso Escrita Admin (Update)" ON public.%I FOR UPDATE TO authenticated USING (public.is_admin()) WITH CHECK (public.is_admin());', t);
        EXECUTE format('CREATE POLICY "Acesso Escrita Admin (Delete)" ON public.%I FOR DELETE TO authenticated USING (public.is_admin());', t);
    END LOOP;
END $$;


-- 4.3 Goals Distribution Policies
-- Cleanup
DROP POLICY IF EXISTS "Goals Write Admin" ON public.goals_distribution;
DROP POLICY IF EXISTS "Goals Read Approved" ON public.goals_distribution;
DROP POLICY IF EXISTS "Acesso Total Unificado" ON public.goals_distribution;
DROP POLICY IF EXISTS "Enable read access for all users" ON public.goals_distribution;

-- Unified Policy (Read/Write for Admins AND Approved users - per requirements)
CREATE POLICY "Acesso Total Unificado" ON public.goals_distribution
FOR ALL TO authenticated
USING (public.is_admin() OR public.is_approved())
WITH CHECK (public.is_admin() OR public.is_approved());


-- ==============================================================================
-- 5. SECURITY FIXES (DYNAMIC SEARCH PATH)
-- ==============================================================================
-- Fixes "Function Search Path Mutable" warnings for any existing functions
DO $$
DECLARE
    func_record RECORD;
BEGIN
    FOR func_record IN
        SELECT
            n.nspname AS schema_name,
            p.proname AS function_name,
            pg_get_function_identity_arguments(p.oid) AS args
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'public'
          AND p.proname IN (
              'get_comparison_data',
              'get_filtered_client_base',
              'get_city_view_data',
              'get_comparison_view_data',
              'get_orders_view_data',
              'get_main_charts_data',
              'get_detailed_orders_data',
              'get_innovations_data_v2',
              'get_weekly_view_data',
              'get_innovations_view_data',
              'get_detailed_orders',
              'get_coverage_view_data',
              'get_filtered_client_base_json',
              'get_stock_view_data'
          )
    LOOP
        EXECUTE format('ALTER FUNCTION %I.%I(%s) SET search_path = public',
                       func_record.schema_name, func_record.function_name, func_record.args);
    END LOOP;
END $$;
