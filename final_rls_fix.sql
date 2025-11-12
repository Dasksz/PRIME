-- =================================================================
-- SCRIPT DE CORREÇÃO FINAL PARA POLÍTICAS DE RLS (ROW LEVEL SECURITY)
-- VERSÃO: V8.0
-- OBJETIVO: Unificar as políticas para permitir acesso total (ALL)
-- tanto à 'service_role' (para o uploader) quanto para usuários
-- autenticados e com status 'aprovado', resolvendo o erro de INSERT.
-- =================================================================

-- ETAPA 1: FUNÇÃO AUXILIAR OTIMIZADA
-- Esta função verifica o status do chamador de forma segura, evitando recursão.
CREATE OR REPLACE FUNCTION public.is_caller_approved()
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
-- Define um search_path estático para segurança e performance.
SET search_path = public
AS $$
  SELECT EXISTS (
    SELECT 1 FROM profiles WHERE id = auth.uid() AND status = 'aprovado'
  );
$$;


-- =================================================================
-- ETAPA 2: APLICAR POLÍTICAS UNIFICADAS PARA CADA TABELA DE DADOS
-- =================================================================

-- Padrão aplicado a cada tabela:
-- 1. Habilita RLS.
-- 2. Apaga TODAS as políticas antigas para garantir um estado limpo.
-- 3. Cria uma ÚNICA política 'FOR ALL' que cobre todos os casos (SELECT, INSERT, UPDATE, DELETE).

-- Tabela: data_detailed
ALTER TABLE public.data_detailed ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Apenas usuários aprovados podem acessar" ON public.data_detailed;
DROP POLICY IF EXISTS "Permitir apagar (delete) para service_role" ON public.data_detailed;
DROP POLICY IF EXISTS "Permitir acesso a service_role ou usuários aprovados" ON public.data_detailed;
CREATE POLICY "Permitir acesso a service_role ou usuários aprovados" ON public.data_detailed
FOR ALL USING (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
);

-- Tabela: data_history
ALTER TABLE public.data_history ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Apenas usuários aprovados podem acessar" ON public.data_history;
DROP POLICY IF EXISTS "Permitir apagar (delete) para service_role" ON public.data_history;
DROP POLICY IF EXISTS "Permitir acesso a service_role ou usuários aprovados" ON public.data_history;
CREATE POLICY "Permitir acesso a service_role ou usuários aprovados" ON public.data_history
FOR ALL USING (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
);

-- Tabela: data_clients
ALTER TABLE public.data_clients ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Apenas usuários aprovados podem acessar" ON public.data_clients;
DROP POLICY IF EXISTS "Permitir apagar (delete) para service_role" ON public.data_clients;
DROP POLICY IF EXISTS "Permitir acesso a service_role ou usuários aprovados" ON public.data_clients;
CREATE POLICY "Permitir acesso a service_role ou usuários aprovados" ON public.data_clients
FOR ALL USING (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
);

-- Tabela: data_orders
ALTER TABLE public.data_orders ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Apenas usuários aprovados podem acessar" ON public.data_orders;
DROP POLICY IF EXISTS "Permitir apagar (delete) para service_role" ON public.data_orders;
DROP POLICY IF EXISTS "Permitir acesso a service_role ou usuários aprovados" ON public.data_orders;
CREATE POLICY "Permitir acesso a service_role ou usuários aprovados" ON public.data_orders
FOR ALL USING (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
);

-- Tabela: data_product_details
ALTER TABLE public.data_product_details ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Apenas usuários aprovados podem acessar" ON public.data_product_details;
DROP POLICY IF EXISTS "Permitir apagar (delete) para service_role" ON public.data_product_details;
DROP POLICY IF EXISTS "Permitir acesso a service_role ou usuários aprovados" ON public.data_product_details;
CREATE POLICY "Permitir acesso a service_role ou usuários aprovados" ON public.data_product_details
FOR ALL USING (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
);

-- Tabela: data_active_products
ALTER TABLE public.data_active_products ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Apenas usuários aprovados podem acessar" ON public.data_active_products;
DROP POLICY IF EXISTS "Permitir apagar (delete) para service_role" ON public.data_active_products;
DROP POLICY IF EXISTS "Permitir acesso a service_role ou usuários aprovados" ON public.data_active_products;
CREATE POLICY "Permitir acesso a service_role ou usuários aprovados" ON public.data_active_products
FOR ALL USING (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
);

-- Tabela: data_stock
ALTER TABLE public.data_stock ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Apenas usuários aprovados podem acessar" ON public.data_stock;
DROP POLICY IF EXISTS "Permitir apagar (delete) para service_role" ON public.data_stock;
DROP POLICY IF EXISTS "Permitir acesso a service_role ou usuários aprovados" ON public.data_stock;
CREATE POLICY "Permitir acesso a service_role ou usuários aprovados" ON public.data_stock
FOR ALL USING (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
);

-- Tabela: data_innovations
ALTER TABLE public.data_innovations ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Apenas usuários aprovados podem acessar" ON public.data_innovations;
DROP POLICY IF EXISTS "Permitir apagar (delete) para service_role" ON public.data_innovations;
DROP POLICY IF EXISTS "Permitir acesso a service_role ou usuários aprovados" ON public.data_innovations;
CREATE POLICY "Permitir acesso a service_role ou usuários aprovados" ON public.data_innovations
FOR ALL USING (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
);

-- Tabela: data_metadata
ALTER TABLE public.data_metadata ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Apenas usuários aprovados podem acessar" ON public.data_metadata;
DROP POLICY IF EXISTS "Permitir apagar (delete) para service_role" ON public.data_metadata;
DROP POLICY IF EXISTS "Permitir acesso a service_role ou usuários aprovados" ON public.data_metadata;
CREATE POLICY "Permitir acesso a service_role ou usuários aprovados" ON public.data_metadata
FOR ALL USING (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
);

-- =================================================================
-- ETAPA FINAL: Forçar o Supabase a recarregar o esquema
-- =================================================================
NOTIFY pgrst, 'reload schema';
