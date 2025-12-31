-- ==============================================================================
-- SECURITY FIX: RLS & PERMISSIONS
-- Descrição: Configuração de segurança para impedir vazamento de dados e escrita não autorizada.
-- ==============================================================================

-- 1. Helper Functions (Funções Auxiliares de Segurança)
-- Verifica se o usuário é ADMIN
CREATE OR REPLACE FUNCTION public.is_admin()
RETURNS boolean AS $$
BEGIN
  -- Service Role sempre é admin (para manutenção via servidor, se necessário)
  IF (select auth.role()) = 'service_role' THEN RETURN true; END IF;
  
  -- Verifica tabela de perfis
  RETURN EXISTS (
    SELECT 1 FROM public.profiles 
    WHERE id = (select auth.uid()) 
    AND role = 'admin'
  );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Verifica se o usuário está APROVADO
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
-- 2. Tabela de Perfis (Profiles) - A base da segurança
-- ==============================================================================
-- Garante que ninguém pode alterar seu próprio status/role para "admin" ou "aprovado"
-- Apenas Admins podem alterar perfis de outros usuários.

ALTER TABLE public.profiles ENABLE ROW LEVEL SECURITY;

-- Leitura: Usuário vê seu próprio perfil (para saber status) E Admin vê todos.
DROP POLICY IF EXISTS "Profiles Visibility" ON public.profiles;
CREATE POLICY "Profiles Visibility" ON public.profiles FOR SELECT
USING (
  (select auth.uid()) = id  -- Ver o próprio
  OR public.is_admin()      -- Admin vê tudo
);

-- Escrita: Apenas Admin pode criar/editar perfis (exceto trigger de criação automática que roda como security definer)
DROP POLICY IF EXISTS "Admin Manage Profiles" ON public.profiles;
CREATE POLICY "Admin Manage Profiles" ON public.profiles FOR ALL
USING (public.is_admin())
WITH CHECK (public.is_admin());

-- Auto-Update (Opcional): Usuário pode editar dados inócuos (email, nome) se houver colunas, 
-- mas NÃO status ou role. Como aqui não temos colunas "seguras" separadas, restringimos a Admin.

-- ==============================================================================
-- 3. Tabelas de Dados (Data Tables) - Proteção de Leitura e Escrita
-- ==============================================================================

DO $$
DECLARE
    t text;
BEGIN
    FOR t IN 
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE 'data_%' -- Alvo: data_detailed, data_clients, etc.
    LOOP
        EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY;', t);
        
        -- Remove permissões públicas inseguras (Anon não pode fazer nada)
        EXECUTE format('REVOKE ALL ON public.%I FROM anon;', t);
        EXECUTE format('REVOKE ALL ON public.%I FROM authenticated;', t);
        -- Garante permissões mínimas para users autenticados (RLS vai filtrar o resto)
        EXECUTE format('GRANT SELECT ON public.%I TO authenticated;', t);
        EXECUTE format('GRANT INSERT, UPDATE, DELETE ON public.%I TO authenticated;', t);

        -- Política de LEITURA: Apenas Aprovados (ou Admins, que tbm são aprovados geralmente)
        EXECUTE format('DROP POLICY IF EXISTS "Read Access Approved" ON public.%I;', t);
        EXECUTE format('CREATE POLICY "Read Access Approved" ON public.%I FOR SELECT USING (public.is_approved());', t);

        -- Política de ESCRITA: Apenas Admins
        EXECUTE format('DROP POLICY IF EXISTS "Write Access Admin" ON public.%I;', t);
        EXECUTE format('CREATE POLICY "Write Access Admin" ON public.%I FOR INSERT WITH CHECK (public.is_admin());', t);
        EXECUTE format('DROP POLICY IF EXISTS "Update Access Admin" ON public.%I;', t);
        EXECUTE format('CREATE POLICY "Update Access Admin" ON public.%I FOR UPDATE USING (public.is_admin()) WITH CHECK (public.is_admin());', t);
        EXECUTE format('DROP POLICY IF EXISTS "Delete Access Admin" ON public.%I;', t);
        EXECUTE format('CREATE POLICY "Delete Access Admin" ON public.%I FOR DELETE USING (public.is_admin());', t);
    END LOOP;
END $$;

-- ==============================================================================
-- 4. Tabela de Metas (Goals Distribution) - Proteção Específica
-- ==============================================================================

ALTER TABLE public.goals_distribution ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON public.goals_distribution FROM anon;
REVOKE ALL ON public.goals_distribution FROM authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.goals_distribution TO authenticated;

-- Leitura: Aprovados
DROP POLICY IF EXISTS "Goals Read Approved" ON public.goals_distribution;
CREATE POLICY "Goals Read Approved" ON public.goals_distribution FOR SELECT
USING (public.is_approved());

-- Escrita: Admins
DROP POLICY IF EXISTS "Goals Write Admin" ON public.goals_distribution;
CREATE POLICY "Goals Write Admin" ON public.goals_distribution FOR ALL
USING (public.is_admin())
WITH CHECK (public.is_admin());

-- ==============================================================================
-- 5. RPC (Remote Procedure Call) para Limpeza Rápida (Truncate)
-- ==============================================================================
-- Função segura para limpar tabelas, acessível apenas por Admins

CREATE OR REPLACE FUNCTION public.truncate_table(table_name text)
RETURNS void AS $$
BEGIN
  -- Verificação de segurança manual (Double Check)
  IF NOT public.is_admin() THEN
    RAISE EXCEPTION 'Acesso negado. Apenas administradores podem limpar tabelas.';
  END IF;

  -- Lista branca de tabelas permitidas para evitar SQL Injection
  IF table_name NOT IN (
    'data_detailed', 'data_history', 'data_clients', 'data_orders', 
    'data_product_details', 'data_active_products', 'data_stock', 
    'data_innovations', 'data_metadata', 'goals_distribution'
  ) THEN
    RAISE EXCEPTION 'Tabela não permitida.';
  END IF;

  EXECUTE format('TRUNCATE TABLE public.%I;', table_name);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Permite RPC apenas para autenticados (RLS/Security Definer checa admin internamente)
REVOKE EXECUTE ON FUNCTION public.truncate_table(text) FROM public;
REVOKE EXECUTE ON FUNCTION public.truncate_table(text) FROM anon;
GRANT EXECUTE ON FUNCTION public.truncate_table(text) TO authenticated;
