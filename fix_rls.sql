-- =================================================================
-- SCRIPT DE CORREÇÃO DE POLÍTICAS DE RLS (ROW LEVEL SECURITY)
-- VERSÃO: V7.0
-- OBJETIVO: Unificar as políticas de segurança para permitir acesso
-- tanto à 'service_role' (para o uploader) quanto para usuários
-- autenticados e com status 'aprovado'.
-- =================================================================

-- FUNÇÃO AUXILIAR PARA EVITAR RECURSÃO E SIMPLIFICAR POLÍTICAS
-- Esta função verifica o status do chamador de forma segura.
CREATE OR REPLACE FUNCTION public.is_caller_approved()
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
-- Reduz o escopo de busca para otimização
SET search_path = public
AS $$
  SELECT EXISTS (
    SELECT 1 FROM profiles WHERE id = (SELECT auth.uid()) AND status = 'aprovado'
  );
$$;


-- =================================================================
-- APLICAR POLÍTICAS UNIFICADAS PARA CADA TABELA DE DADOS
-- =================================================================

-- Padrão a ser repetido para cada tabela:
-- 1. Habilita RLS (se ainda não estiver).
-- 2. Apaga todas as políticas antigas para evitar conflitos.
-- 3. Cria uma única política 'FOR ALL' que cobre todos os casos.

-- Tabela: data_detailed
ALTER TABLE public.data_detailed ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Apenas usuários aprovados podem acessar data_detailed" ON public.data_detailed;
DROP POLICY IF EXISTS "Permitir service_role inserir data_detailed" ON public.data_detailed;
DROP POLICY IF EXISTS "Permitir service_role apagar data_detailed" ON public.data_detailed;
DROP POLICY IF EXISTS "Permitir service_role selecionar data_detailed" ON public.data_detailed;
DROP POLICY IF EXISTS "Permitir acesso a service_role ou usuários aprovados" ON public.data_detailed;
CREATE POLICY "Permitir acesso a service_role ou usuários aprovados" ON public.data_detailed
FOR ALL USING (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
) WITH CHECK (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
);

-- Tabela: data_history
ALTER TABLE public.data_history ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Apenas usuários aprovados podem acessar data_history" ON public.data_history;
DROP POLICY IF EXISTS "Permitir service_role inserir data_history" ON public.data_history;
DROP POLICY IF EXISTS "Permitir service_role apagar data_history" ON public.data_history;
DROP POLICY IF EXISTS "Permitir service_role selecionar data_history" ON public.data_history;
DROP POLICY IF EXISTS "Permitir acesso a service_role ou usuários aprovados" ON public.data_history;
CREATE POLICY "Permitir acesso a service_role ou usuários aprovados" ON public.data_history
FOR ALL USING (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
) WITH CHECK (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
);

-- Tabela: data_clients
ALTER TABLE public.data_clients ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Apenas usuários aprovados podem acessar data_clients" ON public.data_clients;
DROP POLICY IF EXISTS "Permitir service_role inserir data_clients" ON public.data_clients;
DROP POLICY IF EXISTS "Permitir service_role apagar data_clients" ON public.data_clients;
DROP POLICY IF EXISTS "Permitir service_role selecionar data_clients" ON public.data_clients;
DROP POLICY IF EXISTS "Permitir acesso a service_role ou usuários aprovados" ON public.data_clients;
CREATE POLICY "Permitir acesso a service_role ou usuários aprovados" ON public.data_clients
FOR ALL USING (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
) WITH CHECK (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
);

-- Tabela: data_orders
ALTER TABLE public.data_orders ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Apenas usuários aprovados podem acessar data_orders" ON public.data_orders;
DROP POLICY IF EXISTS "Permitir service_role inserir data_orders" ON public.data_orders;
DROP POLICY IF EXISTS "Permitir service_role apagar data_orders" ON public.data_orders;
DROP POLICY IF EXISTS "Permitir service_role selecionar data_orders" ON public.data_orders;
DROP POLICY IF EXISTS "Permitir acesso a service_role ou usuários aprovados" ON public.data_orders;
CREATE POLICY "Permitir acesso a service_role ou usuários aprovados" ON public.data_orders
FOR ALL USING (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
) WITH CHECK (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
);

-- Tabela: data_product_details
ALTER TABLE public.data_product_details ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Apenas usuários aprovados podem acessar data_product_details" ON public.data_product_details;
DROP POLICY IF EXISTS "Permitir service_role inserir data_product_details" ON public.data_product_details;
DROP POLICY IF EXISTS "Permitir service_role apagar data_product_details" ON public.data_product_details;
DROP POLICY IF EXISTS "Permitir service_role selecionar data_product_details" ON public.data_product_details;
DROP POLICY IF EXISTS "Permitir acesso a service_role ou usuários aprovados" ON public.data_product_details;
CREATE POLICY "Permitir acesso a service_role ou usuários aprovados" ON public.data_product_details
FOR ALL USING (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
) WITH CHECK (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
);

-- Tabela: data_active_products
ALTER TABLE public.data_active_products ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Apenas usuários aprovados podem acessar data_active_products" ON public.data_active_products;
DROP POLICY IF EXISTS "Permitir service_role inserir data_active_products" ON public.data_active_products;
DROP POLICY IF EXISTS "Permitir service_role apagar data_active_products" ON public.data_active_products;
DROP POLICY IF EXISTS "Permitir service_role selecionar data_active_products" ON public.data_active_products;
DROP POLICY IF EXISTS "Permitir acesso a service_role ou usuários aprovados" ON public.data_active_products;
CREATE POLICY "Permitir acesso a service_role ou usuários aprovados" ON public.data_active_products
FOR ALL USING (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
) WITH CHECK (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
);

-- Tabela: data_stock
ALTER TABLE public.data_stock ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Apenas usuários aprovados podem acessar data_stock" ON public.data_stock;
DROP POLICY IF EXISTS "Permitir service_role inserir data_stock" ON public.data_stock;
DROP POLICY IF EXISTS "Permitir service_role apagar data_stock" ON public.data_stock;
DROP POLICY IF EXISTS "Permitir service_role selecionar data_stock" ON public.data_stock;
DROP POLICY IF EXISTS "Permitir acesso a service_role ou usuários aprovados" ON public.data_stock;
CREATE POLICY "Permitir acesso a service_role ou usuários aprovados" ON public.data_stock
FOR ALL USING (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
) WITH CHECK (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
);

-- Tabela: data_innovations
ALTER TABLE public.data_innovations ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Apenas usuários aprovados podem acessar data_innovations" ON public.data_innovations;
DROP POLICY IF EXISTS "Permitir service_role inserir data_innovations" ON public.data_innovations;
DROP POLICY IF EXISTS "Permitir service_role apagar data_innovations" ON public.data_innovations;
DROP POLICY IF EXISTS "Permitir service_role selecionar data_innovations" ON public.data_innovations;
DROP POLICY IF EXISTS "Permitir acesso a service_role ou usuários aprovados" ON public.data_innovations;
CREATE POLICY "Permitir acesso a service_role ou usuários aprovados" ON public.data_innovations
FOR ALL USING (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
) WITH CHECK (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
);

-- Tabela: data_metadata
ALTER TABLE public.data_metadata ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Apenas usuários aprovados podem acessar data_metadata" ON public.data_metadata;
DROP POLICY IF EXISTS "Permitir service_role inserir data_metadata" ON public.data_metadata;
DROP POLICY IF EXISTS "Permitir service_role apagar data_metadata" ON public.data_metadata;
DROP POLICY IF EXISTS "Permitir service_role selecionar data_metadata" ON public.data_metadata;
DROP POLICY IF EXISTS "Permitir acesso a service_role ou usuários aprovados" ON public.data_metadata;
CREATE POLICY "Permitir acesso a service_role ou usuários aprovados" ON public.data_metadata
FOR ALL USING (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
) WITH CHECK (
    (auth.role() = 'service_role') OR (public.is_caller_approved())
);

-- =================================================================
-- ETAPA FINAL: Forçar o Supabase a reler o esquema
-- =================================================================
NOTIFY pgrst, 'reload schema';
