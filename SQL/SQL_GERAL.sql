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

-- 1.1 Tabelas de Dimensão
-- Tabela de Vendedores
create table if not exists public.dim_vendedores (
  codusur text primary key,
  nome text not null
);

-- Tabela de Supervisores
create table if not exists public.dim_supervisores (
  codsupervisor text primary key,
  superv text not null
);

-- Tabela de Fornecedores
create table if not exists public.dim_fornecedores (
  codfor text primary key,
  fornecedor text not null,
  observacaofor text -- Pasta
);

-- Tabela de Produtos (Antiga data_product_details)
create table if not exists public.dim_produtos (
  code text primary key,
  descricao text,
  fornecedor text,
  codfor text,
  dtcadastro timestamp with time zone,
  pasta text
);


-- 1.2 Tabela de Vendas Detalhadas (Mês Atual) - Fato
create table if not exists public.data_detailed (
  id uuid default uuid_generate_v4 () primary key,
  pedido text,
  produto text, -- FK para dim_produtos
  codfor text, -- FK para dim_fornecedores
  codusur text, -- FK para dim_vendedores
  codcli text,
  qtvenda numeric,
  codsupervisor text, -- FK para dim_supervisores
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

-- 1.3 Tabela de Histórico de Vendas (Trimestre) - Fato
create table if not exists public.data_history (
  id uuid default uuid_generate_v4 () primary key,
  pedido text,
  produto text,
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

-- 1.4 Tabela de Clientes
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

-- 1.5 Tabela de Pedidos Agregados
create table if not exists public.data_orders (
  id uuid default uuid_generate_v4 () primary key,
  pedido text unique,
  codcli text,
  cliente_nome text,
  cidade text,
  codusur text,
  codsupervisor text,
  dtped timestamp with time zone,
  dtsaida timestamp with time zone,
  posicao text,
  vlvenda numeric,
  totpesoliq numeric,
  filial text,
  tipovenda text,
  codfors_list text[]
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

-- Migração e Limpeza de Esquema Antigo
do $$
BEGIN
    -- Renomeia a tabela de detalhes de produtos para o novo padrão de dimensão, se 'dim_produtos' ainda não existir.
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'data_product_details' AND table_schema = 'public') AND
       NOT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dim_produtos' AND table_schema = 'public') THEN
        ALTER TABLE public.data_product_details RENAME TO dim_produtos;
    END IF;

    -- Remove colunas redundantes da tabela de vendas detalhadas
    ALTER TABLE public.data_detailed DROP COLUMN IF EXISTS nome;
    ALTER TABLE public.data_detailed DROP COLUMN IF EXISTS superv;
    ALTER TABLE public.data_detailed DROP COLUMN IF EXISTS descricao;
    ALTER TABLE public.data_detailed DROP COLUMN IF EXISTS fornecedor;
    ALTER TABLE public.data_detailed DROP COLUMN IF EXISTS observacaofor;

    -- Remove colunas redundantes da tabela de histórico de vendas
    ALTER TABLE public.data_history DROP COLUMN IF EXISTS nome;
    ALTER TABLE public.data_history DROP COLUMN IF EXISTS superv;
    ALTER TABLE public.data_history DROP COLUMN IF EXISTS descricao;
    ALTER TABLE public.data_history DROP COLUMN IF EXISTS fornecedor;
    ALTER TABLE public.data_history DROP COLUMN IF EXISTS observacaofor;

    -- Remove colunas redundantes da tabela de pedidos
    ALTER TABLE public.data_orders DROP COLUMN IF EXISTS nome;
    ALTER TABLE public.data_orders DROP COLUMN IF EXISTS superv;
    ALTER TABLE public.data_orders DROP COLUMN IF EXISTS fornecedores_str;
    ALTER TABLE public.data_orders DROP COLUMN IF EXISTS fornecedores_list;

    -- Adiciona colunas que podem não existir em esquemas antigos (para idempotência)
    ALTER TABLE public.data_orders ADD COLUMN IF NOT EXISTS tipovenda text;
    ALTER TABLE public.data_orders ADD COLUMN IF NOT EXISTS codfors_list text[];
    ALTER TABLE public.data_orders ADD COLUMN IF NOT EXISTS codusur text;
    ALTER TABLE public.data_orders ADD COLUMN IF NOT EXISTS codsupervisor text;

    -- Garante que a coluna 'pasta' existe na nova tabela de dimensão
    ALTER TABLE public.dim_produtos ADD COLUMN IF NOT EXISTS pasta text;
END $$;

-- ==============================================================================
-- 2. HELPER FUNCTIONS
-- ==============================================================================
-- 2.1 Check is_admin
create or replace function public.is_admin () RETURNS boolean as $$
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
$$ LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public;

-- 2.2 Check is_approved
create or replace function public.is_approved () RETURNS boolean as $$
BEGIN
  IF (select auth.role()) = 'service_role' THEN RETURN true; END IF;

  RETURN EXISTS (
    SELECT 1 FROM public.profiles 
    WHERE id = (select auth.uid()) 
    AND status = 'aprovado'
  );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public;

-- 2.3 Truncate Table (Secure)
create or replace function public.truncate_table (table_name text) RETURNS void as $$
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
$$ LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public;

-- 2.4 Get Initial Dashboard Data (Stub)
create or replace function public.get_initial_dashboard_data () RETURNS json as $$
BEGIN
  return '{}'::json;
end;
$$ LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public;

-- ==============================================================================
-- 3. TRIGGERS
-- ==============================================================================
-- 3.1 Handle New User (Create Profile)
create or replace function public.handle_new_user () RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
BEGIN
  insert into public.profiles (id, email, status)
  values (new.id, new.email, 'pendente');
  return new;
end;
$$;

drop trigger IF exists on_auth_user_created on auth.users;

create trigger on_auth_user_created
after INSERT on auth.users for EACH row
execute PROCEDURE public.handle_new_user ();

-- ==============================================================================
-- 4. ROW LEVEL SECURITY (RLS) & POLICIES
-- ==============================================================================
-- Enable RLS on all tables
do $$
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
drop policy IF exists "Profiles Visibility" on public.profiles;

drop policy IF exists "Admin Manage Profiles" on public.profiles;

drop policy IF exists "Users can view own profile" on public.profiles;

drop policy IF exists "Users can update own profile" on public.profiles;

drop policy IF exists "Profiles Unified Select" on public.profiles;

drop policy IF exists "Profiles Unified Update" on public.profiles;

drop policy IF exists "Profiles Unified Insert" on public.profiles;

drop policy IF exists "Profiles Unified Delete" on public.profiles;

-- Create Unified Policies
-- Select: Users see own, Admins see all
create policy "Profiles Unified Select" on public.profiles for
select
  to authenticated using (
    (
      select
        auth.uid ()
    ) = id
    or public.is_admin ()
  );

-- Update: Users update own, Admins update all
create policy "Profiles Unified Update" on public.profiles
for update
  to authenticated using (
    (
      select
        auth.uid ()
    ) = id
    or public.is_admin ()
  )
with
  check (
    (
      select
        auth.uid ()
    ) = id
    or public.is_admin ()
  );

-- Insert: Admins only (Users created via trigger)
create policy "Profiles Unified Insert" on public.profiles for INSERT to authenticated
with
  check (public.is_admin ());

-- Delete: Admins only
create policy "Profiles Unified Delete" on public.profiles for DELETE to authenticated using (public.is_admin ());

-- 4.2 Data Tables Policies (Standardized: Read=Approved|Admin, Write=Admin)
do $$
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
drop policy IF exists "Goals Write Admin" on public.goals_distribution;

drop policy IF exists "Goals Read Approved" on public.goals_distribution;

drop policy IF exists "Acesso Total Unificado" on public.goals_distribution;

drop policy IF exists "Enable read access for all users" on public.goals_distribution;

-- Unified Policy (Read/Write for Admins AND Approved users - per requirements)
create policy "Acesso Total Unificado" on public.goals_distribution for all to authenticated using (
  public.is_admin ()
  or public.is_approved ()
)
with
  check (
    public.is_admin ()
    or public.is_approved ()
  );

-- ==============================================================================
-- 5. UPDATED FUNCTIONS (with Normalization)
-- ==============================================================================

-- get_filtered_client_base_json (JSONB version)
CREATE OR REPLACE FUNCTION public.get_filtered_client_base_json(p_filters jsonb)
 RETURNS TABLE(codigo_cliente text)
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
    p_supervisor_filter TEXT := NULLIF(p_filters->>'supervisor', '');
    p_sellers_filter TEXT[] := CASE
        WHEN p_filters ? 'sellers' AND jsonb_typeof(p_filters->'sellers') = 'array'
        THEN ARRAY(SELECT jsonb_array_elements_text(p_filters->'sellers'))
        ELSE NULL
    END;
    p_rede_group_filter TEXT := NULLIF(p_filters->>'rede_group', '');
    p_redes_filter TEXT[] := CASE
        WHEN p_filters ? 'redes' AND jsonb_typeof(p_filters->'redes') = 'array'
        THEN ARRAY(SELECT jsonb_array_elements_text(p_filters->'redes'))
        ELSE NULL
    END;
    p_city_filter TEXT := NULLIF(p_filters->>'city', '');
    p_filial_filter TEXT := NULLIF(p_filters->>'filial', '');
BEGIN
    RETURN QUERY
    SELECT c.codigo_cliente
    FROM data_clients c
    WHERE
        (p_city_filter IS NULL OR c.cidade ILIKE p_city_filter)
        AND
        (p_rede_group_filter IS NULL OR
         (p_rede_group_filter = 'sem_rede' AND (c.ramo IS NULL OR c.ramo = 'N/A')) OR
         (p_rede_group_filter = 'com_rede' AND (p_redes_filter IS NULL OR c.ramo = ANY(p_redes_filter)))
        )
        AND
        (p_filial_filter IS NULL OR p_filial_filter = 'ambas' OR EXISTS (
             SELECT 1 FROM data_detailed d WHERE d.codcli = c.codigo_cliente AND d.filial = p_filial_filter
            UNION ALL
            SELECT 1 FROM data_history h WHERE h.codcli = c.codigo_cliente AND h.filial = p_filial_filter
            LIMIT 1
        ))
        AND
        (p_supervisor_filter IS NULL OR EXISTS (
            SELECT 1 FROM data_detailed d JOIN dim_supervisores ds ON d.codsupervisor = ds.codsupervisor WHERE d.codcli = c.codigo_cliente AND ds.superv = p_supervisor_filter
            UNION ALL
            SELECT 1 FROM data_history h JOIN dim_supervisores ds ON h.codsupervisor = ds.codsupervisor WHERE h.codcli = c.codigo_cliente AND ds.superv = p_supervisor_filter
            LIMIT 1
        ))
        AND
        (p_sellers_filter IS NULL OR EXISTS (
            SELECT 1 FROM data_detailed d JOIN dim_vendedores dv ON d.codusur = dv.codusur WHERE d.codcli = c.codigo_cliente AND dv.nome = ANY(p_sellers_filter)
            UNION ALL
            SELECT 1 FROM data_history h JOIN dim_vendedores dv ON h.codusur = dv.codusur WHERE h.codcli = c.codigo_cliente AND dv.nome = ANY(p_sellers_filter)
            LIMIT 1
        ));
END;
$function$;


-- get_comparison_data (JSONB version)
CREATE OR REPLACE FUNCTION public.get_comparison_data(p_supervisor_filter text DEFAULT NULL::text, p_sellers_filter text[] DEFAULT NULL::text[], p_suppliers_filter text[] DEFAULT NULL::text[], p_products_filter text[] DEFAULT NULL::text[], p_pasta_filter text DEFAULT NULL::text, p_city_filter text DEFAULT NULL::text, p_filial_filter text DEFAULT 'ambas'::text, p_rede_group_filter text DEFAULT NULL::text, p_redes_filter text[] DEFAULT NULL::text[])
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
    result JSONB;
    current_month_start DATE;
    history_start_date DATE;
    history_end_date DATE;
BEGIN
    SELECT date_trunc('month', MAX(dtped))::DATE INTO current_month_start FROM data_detailed;
    history_end_date := current_month_start - 1;
    history_start_date := current_month_start - INTERVAL '3 months';

    WITH
    filtered_clients AS (
        SELECT * FROM get_filtered_client_base_json(jsonb_build_object(
            'supervisor', p_supervisor_filter,
            'sellers', p_sellers_filter,
            'rede_group', p_rede_group_filter,
            'redes', p_redes_filter,
            'city', p_city_filter,
            'filial', p_filial_filter
        ))
    ),
    current_sales AS (
        SELECT s.*, df.observacaofor, ds.superv
        FROM data_detailed s
        JOIN filtered_clients fc ON s.codcli = fc.codigo_cliente
        JOIN dim_fornecedores df ON s.codfor = df.codfor
        JOIN dim_supervisores ds ON s.codsupervisor = ds.codsupervisor
        WHERE (p_suppliers_filter IS NULL OR s.codfor = ANY(p_suppliers_filter))
          AND (p_products_filter IS NULL OR s.produto = ANY(p_products_filter))
          AND (p_pasta_filter IS NULL OR df.observacaofor = p_pasta_filter)
    ),
    history_sales AS (
        SELECT s.*, df.observacaofor, ds.superv
        FROM data_history s
        JOIN filtered_clients fc ON s.codcli = fc.codigo_cliente
        JOIN dim_fornecedores df ON s.codfor = df.codfor
        JOIN dim_supervisores ds ON s.codsupervisor = ds.codsupervisor
        WHERE s.dtped BETWEEN history_start_date AND history_end_date
          AND (p_suppliers_filter IS NULL OR s.codfor = ANY(p_suppliers_filter))
          AND (p_products_filter IS NULL OR s.produto = ANY(p_products_filter))
          AND (p_pasta_filter IS NULL OR df.observacaofor = p_pasta_filter)
    ),
    kpis_current AS (
        SELECT
            COALESCE(SUM(CASE WHEN tipovenda IN ('1', '9') THEN vlvenda ELSE 0::numeric END), 0) AS total_fat,
            COALESCE(SUM(totpesoliq), 0) AS total_peso,
            COUNT(DISTINCT codcli) AS total_clients
        FROM current_sales
    ),
    kpis_history_monthly AS (
        SELECT
            date_trunc('month', dtped) as month,
            COALESCE(SUM(CASE WHEN tipovenda IN ('1', '9') THEN vlvenda ELSE 0::numeric END), 0) AS total_fat,
            COALESCE(SUM(totpesoliq), 0) AS total_peso,
            COUNT(DISTINCT codcli) AS total_clients
        FROM history_sales
        GROUP BY 1
    ),
    kpis_history_avg AS (
        SELECT
            COALESCE(AVG(total_fat), 0) AS avg_fat,
            COALESCE(AVG(total_peso), 0) AS avg_peso,
            COALESCE(AVG(total_clients), 0) AS avg_clients
        FROM kpis_history_monthly
    ),
    history_sales_with_week_num AS (
        SELECT
            s.dtped, s.tipovenda, s.vlvenda,
            floor((extract(day from s.dtped) - 1) / 7) + 1 AS week_of_month
        FROM history_sales s
    ),
    history_avg_by_week_num AS (
        SELECT
            week_of_month,
            COALESCE(SUM(CASE WHEN tipovenda IN ('1', '9') THEN vlvenda ELSE 0::numeric END) / 3.0, 0) AS avg_faturamento
        FROM history_sales_with_week_num
        GROUP BY week_of_month
    ),
    history_supervisor_agg AS (
        SELECT
            s.codsupervisor,
            SUM(CASE WHEN tipovenda IN ('1', '9') THEN vlvenda ELSE 0::numeric END) as total_fat
        FROM history_sales s
        GROUP BY s.codsupervisor
    ),
    week_series AS (
        SELECT
            n as week_num,
            (date_trunc('week', current_month_start)::date + ((n-1) || ' weeks')::interval) as week_start,
            (date_trunc('week', current_month_start)::date + (n || ' weeks')::interval - '1 day'::interval) as week_end
        FROM generate_series(1, 6) n
        WHERE (date_trunc('week', current_month_start)::date + ((n-1) || ' weeks')::interval) < (current_month_start + '1 month'::interval)
    ),
    weekly_comparison_chart AS (
        SELECT
            COALESCE(jsonb_agg(
                jsonb_build_object(
                    'week_num', ws.week_num,
                    'current_faturamento', (
                        SELECT COALESCE(SUM(CASE WHEN cs.tipovenda IN ('1', '9') THEN cs.vlvenda ELSE 0::numeric END), 0)
                        FROM current_sales cs
                        WHERE cs.dtped BETWEEN ws.week_start AND ws.week_end
                    ),
                    'history_avg_faturamento', (
                         SELECT COALESCE(haw.avg_faturamento, 0)
                         FROM history_avg_by_week_num haw
                         WHERE haw.week_of_month = ws.week_num
                    )
                ) ORDER BY ws.week_num
            ), '[]'::jsonb) AS data
        FROM week_series ws
    ),
    supervisor_comparison_table AS (
        SELECT COALESCE(jsonb_agg(t ORDER BY current_faturamento DESC), '[]'::jsonb) AS data
        FROM (
            SELECT
                ds.superv,
                COALESCE(SUM(CASE WHEN s.tipovenda IN ('1', '9') THEN s.vlvenda ELSE 0::numeric END), 0) AS current_faturamento,
                (SELECT COALESCE(hsa.total_fat / 3.0, 0) FROM history_supervisor_agg hsa WHERE hsa.codsupervisor = s.codsupervisor) AS history_avg_faturamento
            FROM current_sales s
            JOIN dim_supervisores ds ON s.codsupervisor = ds.codsupervisor
            WHERE s.codsupervisor IS NOT NULL
            GROUP BY s.codsupervisor, ds.superv
        ) t
    )
    SELECT jsonb_build_object(
        'kpis', (
            SELECT jsonb_build_object(
                'current_total_fat', kc.total_fat,
                'history_avg_fat', kha.avg_fat,
                'current_total_peso', kc.total_peso,
                'history_avg_peso', kha.avg_peso,
                'current_clients', kc.total_clients,
                'history_avg_clients', kha.avg_clients,
                'current_ticket', CASE WHEN kc.total_clients > 0 THEN kc.total_fat / kc.total_clients ELSE 0 END,
                'history_avg_ticket', CASE WHEN kha.avg_clients > 0 THEN kha.avg_fat / kha.avg_clients ELSE 0 END
            ) FROM kpis_current kc, kpis_history_avg kha
        ),
        'charts', (
            SELECT jsonb_build_object('weekly_comparison', wc.data) FROM weekly_comparison_chart wc
        ),
        'tables', (
            SELECT jsonb_build_object('supervisor_comparison', sc.data) FROM supervisor_comparison_table sc
        )
    )
    INTO result;

    RETURN result;
END;
$function$;

-- get_city_view_data
CREATE OR REPLACE FUNCTION public.get_city_view_data(p_supervisor_filter text DEFAULT ''::text, p_sellers_filter text[] DEFAULT NULL::text[], p_rede_group_filter text DEFAULT ''::text, p_redes_filter text[] DEFAULT NULL::text[], p_city_filter text DEFAULT ''::text, p_codcli_filter text DEFAULT ''::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
    result JSONB;
    current_month_start DATE;
BEGIN
    SELECT date_trunc('month', MAX(dtped))::DATE INTO current_month_start FROM data_detailed;

    WITH
    clients_base AS (
        SELECT c.codigo_cliente, c.ramo, c.rca1, c.cidade, c.bairro, c.fantasia, c.razaosocial, c.ultimacompra, c.datacadastro, c.bloqueio
        FROM data_clients c
        WHERE (p_rede_group_filter = '')
           OR (p_rede_group_filter = 'sem_rede' AND (c.ramo IS NULL OR c.ramo = 'N/A'))
           OR (p_rede_group_filter = 'com_rede' AND (p_redes_filter IS NULL OR c.ramo = ANY(p_redes_filter)))
    ),
    filtered_sales AS (
        SELECT s.codcli, s.cidade, s.bairro, s.vlvenda, df.observacaofor, s.tipovenda
        FROM data_detailed s
        JOIN clients_base cb ON s.codcli = cb.codigo_cliente
        LEFT JOIN dim_supervisores ds ON s.codsupervisor = ds.codsupervisor
        LEFT JOIN dim_vendedores dv ON s.codusur = dv.codusur
        LEFT JOIN dim_fornecedores df ON s.codfor = df.codfor
        WHERE s.dtped >= current_month_start
          AND (p_supervisor_filter = '' OR ds.superv = p_supervisor_filter)
          AND (p_sellers_filter IS NULL OR dv.nome = ANY(p_sellers_filter))
          AND (p_city_filter = '' OR s.cidade ILIKE p_city_filter)
          AND (p_codcli_filter = '' OR s.codcli = p_codcli_filter)
    ),
    client_sales_agg AS (
        SELECT
            codcli,
            SUM(vlvenda) as total_faturamento,
            SUM(CASE WHEN observacaofor = 'PEPSICO' THEN vlvenda ELSE 0 END) as pepsico_total,
            SUM(CASE WHEN observacaofor = 'MULTIMARCAS' THEN vlvenda ELSE 0 END) as multimarcas_total
        FROM filtered_sales
        WHERE tipovenda IN ('1', '9')
        GROUP BY codcli
    ),
    client_status AS (
        SELECT
            c.codigo_cliente, c.fantasia, c.razaosocial, c.cidade, c.bairro, c.rca1, c.ultimacompra, c.datacadastro,
            COALESCE(csa.total_faturamento, 0) as total_faturamento,
            COALESCE(csa.pepsico_total, 0) as pepsico,
            COALESCE(csa.multimarcas_total, 0) as multimarcas,
            (csa.codcli IS NOT NULL AND csa.total_faturamento > 0) as is_active,
            (c.datacadastro::date >= current_month_start) as is_new
        FROM clients_base c
        LEFT JOIN client_sales_agg csa ON c.codigo_cliente = csa.codcli
        WHERE c.codigo_cliente <> '6720' AND c.bloqueio <> 'S'
          AND NOT (c.rca1 = '53' AND c.razaosocial NOT ILIKE '%AMERICANAS%')
    ),
    active_clients_table AS (
        SELECT COALESCE(jsonb_agg(act ORDER BY total_faturamento DESC), '[]'::jsonb) as data
        FROM client_status act
        WHERE is_active
    ),
    inactive_clients_table AS (
        SELECT COALESCE(jsonb_agg(ict ORDER BY ict.ultimacompra::date DESC), '[]'::jsonb) as data
        FROM client_status ict
        WHERE NOT is_active
    ),
    top_10_chart AS (
        SELECT COALESCE(jsonb_agg(chart_data), '[]'::jsonb) as data
        FROM (
            SELECT
                CASE WHEN p_city_filter <> '' THEN bairro ELSE cidade END as label,
                SUM(vlvenda) as total
            FROM filtered_sales
            WHERE tipovenda IN ('1', '9')
            GROUP BY 1
            ORDER BY total DESC
            LIMIT 10
        ) chart_data
    )
    SELECT jsonb_build_object(
        'total_faturamento', (SELECT SUM(vlvenda) FROM filtered_sales WHERE tipovenda IN ('1', '9')),
        'status_chart', (
            SELECT jsonb_build_object(
                'active', COUNT(*) FILTER (WHERE is_active),
                'inactive', COUNT(*) FILTER (WHERE NOT is_active)
            ) FROM client_status
        ),
        'top_10_chart', (SELECT data FROM top_10_chart),
        'active_clients', (SELECT data FROM active_clients_table),
        'inactive_clients', (SELECT data FROM inactive_clients_table)
    )
    INTO result;

    RETURN result;
END;
$function$;

-- get_weekly_view_data
CREATE OR REPLACE FUNCTION public.get_weekly_view_data(p_supervisors_filter text[] DEFAULT NULL::text[], p_pasta_filter text DEFAULT NULL::text[])
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
    result JSONB;
    current_month_start DATE;
    previous_month_start DATE;
    previous_month_end DATE;
BEGIN
    SELECT date_trunc('month', MAX(dtped))::DATE INTO current_month_start FROM data_detailed;
    previous_month_start := current_month_start - INTERVAL '1 month';
    previous_month_end := current_month_start - INTERVAL '1 day';

    WITH
    current_sales AS (
        SELECT s.*, ds.superv, dv.nome, df.observacaofor
        FROM data_detailed s
        LEFT JOIN dim_supervisores ds ON s.codsupervisor = ds.codsupervisor
        LEFT JOIN dim_vendedores dv ON s.codusur = dv.codusur
        LEFT JOIN dim_fornecedores df ON s.codfor = df.codfor
        WHERE s.dtped >= current_month_start
          AND (p_supervisors_filter IS NULL OR ds.superv = ANY(p_supervisors_filter))
          AND (p_pasta_filter IS NULL OR df.observacaofor = p_pasta_filter)
    ),
    previous_month_sales AS (
        SELECT s.*, ds.superv
        FROM data_history s
        JOIN dim_supervisores ds ON s.codsupervisor = ds.codsupervisor
        WHERE s.dtped BETWEEN previous_month_start AND previous_month_end
          AND (p_supervisors_filter IS NULL OR ds.superv = ANY(p_supervisors_filter))
    ),
    total_revenue AS (
        SELECT COALESCE(SUM(vlvenda), 0) as total FROM current_sales WHERE tipovenda IN ('1', '9')
    ),
    sales_by_week_day AS (
        SELECT
            EXTRACT(WEEK FROM dtped) - EXTRACT(WEEK FROM date_trunc('month', dtped)) + 1 as week_num,
            EXTRACT(ISODOW FROM dtped) as day_of_week,
            SUM(vlvenda) as daily_total
        FROM current_sales
        WHERE tipovenda IN ('1', '9') AND EXTRACT(ISODOW FROM dtped) BETWEEN 1 AND 5
        GROUP BY 1, 2
    ),
    historical_best_day AS (
        SELECT
            superv,
            EXTRACT(ISODOW FROM dtped) as day_of_week,
            MAX(daily_total) as best_day_total
        FROM (
            SELECT superv, dtped, SUM(vlvenda) as daily_total
            FROM previous_month_sales
            WHERE tipovenda IN ('1', '9') AND superv <> 'BALCAO'
            GROUP BY superv, dtped
        ) as daily_sales
        GROUP BY superv, day_of_week
    ),
    positivacao_rank AS (
        SELECT nome, COUNT(DISTINCT codcli) as total
        FROM current_sales
        WHERE nome IS NOT NULL AND superv <> 'BALCAO'
        GROUP BY nome
        ORDER BY total DESC
        LIMIT 10
    ),
    top_sellers_rank AS (
        SELECT nome, SUM(vlvenda) as total
        FROM current_sales
        WHERE nome IS NOT NULL AND superv <> 'BALCAO' AND tipovenda IN ('1', '9')
        GROUP BY nome
        ORDER BY total DESC
        LIMIT 10
    ),
    mix_rank_cte AS (
        SELECT
            nome,
            codcli,
            COUNT(DISTINCT produto) as mix
        FROM current_sales
        WHERE nome IS NOT NULL AND superv <> 'BALCAO'
        GROUP BY nome, codcli
    ),
    avg_mix_rank AS (
        SELECT
            nome,
            AVG(mix) as avg_mix
        FROM mix_rank_cte
        GROUP BY nome
        ORDER BY avg_mix DESC
        LIMIT 10
    )
    SELECT jsonb_build_object(
        'total_faturamento', (SELECT total FROM total_revenue),
        'sales_by_week_day', (SELECT COALESCE(jsonb_agg(swd), '[]'::jsonb) FROM sales_by_week_day swd),
        'historical_best_days', (SELECT COALESCE(jsonb_agg(hbd), '[]'::jsonb) FROM historical_best_day hbd),
        'positivacao_rank', (SELECT COALESCE(jsonb_agg(pr), '[]'::jsonb) FROM positivacao_rank pr),
        'top_sellers_rank', (SELECT COALESCE(jsonb_agg(tsr), '[]'::jsonb) FROM top_sellers_rank tsr),
        'mix_rank', (SELECT COALESCE(jsonb_agg(amr), '[]'::jsonb) FROM avg_mix_rank amr)
    )
    INTO result;

    RETURN result;
END;
$function$;


-- get_main_charts_data
CREATE OR REPLACE FUNCTION public.get_main_charts_data(p_supervisor text DEFAULT NULL::text, p_sellers text[] DEFAULT NULL::text[])
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
    sales_by_person jsonb;
    sales_by_pasta jsonb;
    trend_data jsonb;
    top_products_fat jsonb;
    top_products_peso jsonb;
BEGIN
    IF p_supervisor IS NOT NULL THEN
        SELECT jsonb_agg(jsonb_build_object('person', dv.nome, 'total', total)) INTO sales_by_person FROM (
            SELECT s.codusur, SUM(s.vlvenda) AS total FROM public.data_detailed s LEFT JOIN public.dim_supervisores ds ON s.codsupervisor = ds.codsupervisor WHERE ds.superv = p_supervisor GROUP BY s.codusur ORDER BY total DESC
        ) AS sub
        LEFT JOIN public.dim_vendedores dv ON sub.codusur = dv.codusur;
    ELSE
        SELECT jsonb_agg(jsonb_build_object('person', ds.superv, 'total', total)) INTO sales_by_person FROM (
            SELECT codsupervisor, SUM(vlvenda) AS total FROM public.data_detailed WHERE codsupervisor IS NOT NULL GROUP BY codsupervisor ORDER BY total DESC
        ) AS sub
        LEFT JOIN public.dim_supervisores ds ON sub.codsupervisor = ds.codsupervisor;
    END IF;

    SELECT jsonb_agg(jsonb_build_object('pasta', df.observacaofor, 'total', total)) INTO sales_by_pasta FROM (
        SELECT codfor, SUM(vlvenda) AS total FROM public.data_detailed WHERE codfor IS NOT NULL GROUP BY codfor ORDER BY total DESC LIMIT 10
    ) AS sub
    LEFT JOIN public.dim_fornecedores df ON sub.codfor = df.codfor;

    SELECT jsonb_build_object('avg_revenue', 150000, 'trend_revenue', 180000) INTO trend_data;

    SELECT jsonb_agg(jsonb_build_object('produto', sub.produto, 'descricao', dp.descricao, 'faturamento', total)) INTO top_products_fat FROM (
        SELECT produto, SUM(vlvenda) as total FROM public.data_detailed GROUP BY produto ORDER BY total DESC LIMIT 10
    ) AS sub
    JOIN public.dim_produtos dp ON sub.produto = dp.code;

    SELECT jsonb_agg(jsonb_build_object('produto', sub.produto, 'descricao', dp.descricao, 'peso', total)) INTO top_products_peso FROM (
        SELECT produto, SUM(totpesoliq) as total FROM public.data_detailed GROUP BY produto ORDER BY total DESC LIMIT 10
    ) AS sub
    JOIN public.dim_produtos dp ON sub.produto = dp.code;

    RETURN jsonb_build_object(
        'sales_by_supervisor', sales_by_person,
        'sales_by_pasta', sales_by_pasta,
        'trend', trend_data,
        'top_10_products_faturamento', top_products_fat,
        'top_10_products_peso', top_products_peso
    );
END;
$function$;

-- get_orders_view_data
CREATE OR REPLACE FUNCTION public.get_orders_view_data(p_supervisor text DEFAULT NULL::text, p_sellers text[] DEFAULT NULL::text[], p_position text DEFAULT NULL::text, p_client_code text DEFAULT NULL::text, p_supplier_code text DEFAULT NULL::text, p_rede_group text DEFAULT NULL::text, p_redes text[] DEFAULT NULL::text[], p_sale_types text[] DEFAULT NULL::text[], p_page_number integer DEFAULT 1, p_items_per_page integer DEFAULT 50)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
    result jsonb;
BEGIN
    WITH filtered_clients AS (
        SELECT codigo_cliente
        FROM public.data_clients
        WHERE
            CASE
                WHEN p_rede_group = 'com_rede' THEN
                    ramo IS NOT NULL AND ramo <> 'N/A' AND (p_redes IS NULL OR ramo = ANY(p_redes))
                WHEN p_rede_group = 'sem_rede' THEN
                    (ramo IS NULL OR ramo = 'N/A')
                ELSE TRUE
            END
    ),
    filtered_orders AS (
        SELECT o.*, dv.nome, ds.superv
        FROM public.data_orders o
        LEFT JOIN public.dim_vendedores dv ON o.codusur = dv.codusur
        LEFT JOIN public.dim_supervisores ds ON o.codsupervisor = ds.codsupervisor
        WHERE
            (p_supervisor IS NULL OR ds.superv = p_supervisor)
            AND (p_sellers IS NULL OR dv.nome = ANY(p_sellers))
            AND (p_position IS NULL OR o.posicao = p_position)
            AND (p_client_code IS NULL OR o.codcli = p_client_code)
            AND (p_supplier_code IS NULL OR p_supplier_code = ANY(o.codfors_list))
            AND (p_sale_types IS NULL OR o.tipovenda = ANY(p_sale_types))
            AND (o.codcli IN (SELECT codigo_cliente FROM filtered_clients))
    )
    SELECT jsonb_build_object(
        'total_count', (SELECT COUNT(*) FROM filtered_orders),
        'data', (SELECT jsonb_agg(t.*) FROM (
            SELECT *
            FROM filtered_orders
            ORDER BY dtped DESC
            LIMIT p_items_per_page
            OFFSET ((p_page_number - 1) * p_items_per_page)
        ) t)
    )
    INTO result;

    RETURN result;
END;
$function$;


-- get_stock_view_data
-- get_stock_view_data (corrigido)
CREATE OR REPLACE FUNCTION public.get_stock_view_data(p_supervisor_filter text DEFAULT ''::text, p_sellers_filter text[] DEFAULT NULL::text[], p_suppliers_filter text[] DEFAULT NULL::text[], p_products_filter text[] DEFAULT NULL::text[], p_rede_group_filter text DEFAULT ''::text, p_redes_filter text[] DEFAULT NULL::text[], p_filial_filter text DEFAULT 'ambas'::text, p_city_filter text DEFAULT ''::text, p_custom_days integer DEFAULT 0)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
    result JSONB;
    current_month_start DATE;
    last_sale_date DATE;
    history_start_date DATE;
    history_end_date DATE;
    first_ever_sale_date DATE;
    max_working_days INTEGER;
BEGIN
    SELECT date_trunc('month', MAX(dtped))::DATE, MAX(dtped)::DATE
    INTO current_month_start, last_sale_date
    FROM data_detailed;

    history_end_date := current_month_start - INTERVAL '1 day';
    history_start_date := current_month_start - INTERVAL '3 months';

    SELECT MIN(d.dtped) INTO first_ever_sale_date FROM (
        SELECT dtped FROM data_detailed UNION ALL SELECT dtped FROM data_history
    ) d WHERE dtped IS NOT NULL;

    SELECT COUNT(*) INTO max_working_days
    FROM generate_series(first_ever_sale_date, last_sale_date, '1 day') d
    WHERE extract(isodow from d) < 6;

    WITH
    final_clients AS (
        SELECT c.codigo_cliente
        FROM data_clients c
        WHERE (p_rede_group_filter = '' OR
              (p_rede_group_filter = 'sem_rede' AND (c.ramo IS NULL OR c.ramo = 'N/A')) OR
              (p_rede_group_filter = 'com_rede' AND (p_redes_filter IS NULL OR c.ramo = ANY(p_redes_filter))))
          AND (p_city_filter = '' OR c.cidade ILIKE p_city_filter)
          AND (p_supervisor_filter = '' OR EXISTS (
            SELECT 1 FROM data_detailed d JOIN dim_supervisores ds ON d.codsupervisor = ds.codsupervisor WHERE d.codcli = c.codigo_cliente AND ds.superv = p_supervisor_filter
            UNION ALL SELECT 1 FROM data_history h JOIN dim_supervisores ds ON h.codsupervisor = ds.codsupervisor WHERE h.codcli = c.codigo_cliente AND ds.superv = p_supervisor_filter LIMIT 1
          ))
          AND (p_sellers_filter IS NULL OR EXISTS (
            SELECT 1 FROM data_detailed d JOIN dim_vendedores dv ON d.codusur = dv.codusur WHERE d.codcli = c.codigo_cliente AND dv.nome = ANY(p_sellers_filter)
            UNION ALL SELECT 1 FROM data_history h JOIN dim_vendedores dv ON h.codusur = dv.codusur WHERE h.codcli = c.codigo_cliente AND dv.nome = ANY(p_sellers_filter) LIMIT 1
          ))
    ),
    all_sales AS (
      SELECT produto, qtvenda_embalagem_master, dtped FROM data_detailed WHERE codcli IN (SELECT codigo_cliente FROM final_clients) AND (p_filial_filter = 'ambas' OR filial = p_filial_filter)
      UNION ALL
      SELECT produto, qtvenda_embalagem_master, dtped FROM data_history WHERE codcli IN (SELECT codigo_cliente FROM final_clients) AND (p_filial_filter = 'ambas' OR filial = p_filial_filter)
    ),
    working_days_ranked AS (
        SELECT d::date as work_date, row_number() OVER (ORDER BY d DESC) as rn
        FROM generate_series(first_ever_sale_date, last_sale_date, '1 day'::interval) d
        WHERE extract(isodow from d) < 6
    ),
    sales_metrics AS (
      SELECT
        s.produto,
        COUNT(DISTINCT s.dtped) FILTER (WHERE extract(isodow from s.dtped) < 6) as product_lifetime_days,
        SUM(CASE WHEN s.dtped >= current_month_start THEN s.qtvenda_embalagem_master ELSE 0 END) as current_month_sales_qty,
        SUM(CASE WHEN s.dtped BETWEEN history_start_date AND history_end_date THEN s.qtvenda_embalagem_master ELSE 0 END) / 3.0 as history_avg_monthly_qty
      FROM all_sales s
      GROUP BY s.produto
    ),
    stock_aggregated AS (
        SELECT product_code, SUM(stock_qty) as total_stock
        FROM data_stock
        WHERE (p_filial_filter = 'ambas' OR filial = p_filial_filter)
        GROUP BY product_code
    ),
    final_data AS (
        SELECT
            pd.code as product_code,
            pd.descricao as product_description,
            pd.fornecedor as supplier_name,
            COALESCE(st.total_stock, 0) as stock_qty,
            COALESCE(sm.current_month_sales_qty, 0) as current_month_sales_qty,
            COALESCE(sm.history_avg_monthly_qty, 0) as history_avg_monthly_qty,
            GREATEST(LEAST(
                CASE WHEN p_custom_days <= 0 OR p_custom_days > sm.product_lifetime_days THEN sm.product_lifetime_days ELSE p_custom_days END,
                max_working_days
            ), 1)::INTEGER as days_divisor
        FROM dim_produtos pd
        LEFT JOIN sales_metrics sm ON pd.code = sm.produto
        LEFT JOIN stock_aggregated st ON pd.code = st.product_code
        WHERE (p_suppliers_filter IS NULL OR pd.codfor = ANY(p_suppliers_filter))
          AND (p_products_filter IS NULL OR pd.code = ANY(p_products_filter))
          AND (COALESCE(st.total_stock, 0) > 0 OR COALESCE(sm.current_month_sales_qty, 0) > 0 OR COALESCE(sm.history_avg_monthly_qty, 0) > 0)
    ),
    daily_sales AS (
        SELECT produto, dtped, SUM(qtvenda_embalagem_master) as total_qty
        FROM all_sales
        GROUP BY produto, dtped
    ),
    product_sales_ranked AS (
        SELECT ds.produto, ds.total_qty, wdr.rn
        FROM daily_sales ds
        LEFT JOIN working_days_ranked wdr ON ds.dtped = wdr.work_date
    ),
    sales_in_window AS (
        SELECT
            fd.product_code,
            SUM(psr.total_qty) as total_sales_for_avg
        FROM final_data fd
        LEFT JOIN product_sales_ranked psr ON fd.product_code = psr.produto AND psr.rn <= fd.days_divisor
        GROUP BY fd.product_code
    ),
    categorized_products AS (
        SELECT
            fd.*,
            COALESCE(siw.total_sales_for_avg, 0) / fd.days_divisor as daily_avg_qty,
            CASE
                WHEN fd.current_month_sales_qty > 0 AND fd.history_avg_monthly_qty > 0 AND fd.current_month_sales_qty >= fd.history_avg_monthly_qty THEN 'growth'
                WHEN fd.current_month_sales_qty > 0 AND fd.history_avg_monthly_qty > 0 AND fd.current_month_sales_qty < fd.history_avg_monthly_qty THEN 'decline'
                WHEN fd.current_month_sales_qty > 0 AND fd.history_avg_monthly_qty = 0 THEN 'new'
                WHEN fd.current_month_sales_qty = 0 AND fd.history_avg_monthly_qty > 0 THEN 'lost'
                ELSE NULL
            END as category
        FROM final_data fd
        LEFT JOIN sales_in_window siw ON fd.product_code = siw.product_code
    )
    SELECT jsonb_build_object(
        'max_working_days', max_working_days,
        'working_days_used', CASE WHEN p_custom_days > 0 AND p_custom_days < max_working_days THEN p_custom_days ELSE max_working_days END,
        'stock_table', (
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'product_code', cp.product_code,
                'product_description', cp.product_description,
                'supplier_name', cp.supplier_name,
                'stock_qty', cp.stock_qty,
                'avg_monthly_qty', cp.history_avg_monthly_qty,
                'daily_avg_qty', cp.daily_avg_qty,
                'trend_days', CASE WHEN cp.daily_avg_qty > 0 THEN (cp.stock_qty / cp.daily_avg_qty) ELSE 999 END
            ) ORDER BY (CASE WHEN cp.daily_avg_qty > 0 THEN (cp.stock_qty / cp.daily_avg_qty) ELSE 999 END) ASC), '[]'::jsonb)
            FROM categorized_products cp
        ),
        'growth_table', (SELECT COALESCE(jsonb_agg(cp ORDER BY (current_month_sales_qty - history_avg_monthly_qty) DESC), '[]'::jsonb) FROM categorized_products cp WHERE category = 'growth'),
        'decline_table', (SELECT COALESCE(jsonb_agg(cp ORDER BY (current_month_sales_qty - history_avg_monthly_qty) ASC), '[]'::jsonb) FROM categorized_products cp WHERE category = 'decline'),
        'new_table', (SELECT COALESCE(jsonb_agg(cp ORDER BY current_month_sales_qty DESC), '[]'::jsonb) FROM categorized_products cp WHERE category = 'new'),
        'lost_table', (SELECT COALESCE(jsonb_agg(cp ORDER BY history_avg_monthly_qty DESC), '[]'::jsonb) FROM categorized_products cp WHERE category = 'lost')
    ) INTO result;

    RETURN result;
END;
$function$;

-- ==============================================================================
-- 5. SECURITY FIXES (DYNAMIC SEARCH PATH)
-- ==============================================================================
-- Fixes "Function Search Path Mutable" warnings for any existing functions
do $$
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
