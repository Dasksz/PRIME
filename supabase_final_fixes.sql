-- =================================================================
-- SCRIPT DE CORREÇÃO FINAL - SUPABASE
-- Execute este script completo no seu editor de SQL do Supabase.
-- Objetivo: Resolver todos os avisos de segurança e performance.
-- =================================================================

-- Configura o caminho de busca para a sessão atual
set search_path = public, auth, realtime;

-- =================================================================
-- PARTE 1: CORREÇÃO DE SEGURANÇA
-- Corrige a função `broadcast_profiles_update` para ter um caminho de busca fixo.
-- =================================================================

CREATE OR REPLACE FUNCTION public.broadcast_profiles_update()
RETURNS TRIGGER
LANGUAGE plpgsql
-- Define um caminho de busca seguro e imutável para a função
SET search_path = 'public', 'realtime'
AS $$
BEGIN
  PERFORM realtime.broadcast_changes(
    'profiles:' || NEW.id::text,
    TG_OP,
    TG_OP,
    TG_TABLE_NAME,
    TG_TABLE_SCHEMA,
    NEW,
    OLD
  );
  RETURN NEW;
END;
$$;


-- =================================================================
-- PARTE 2: CORREÇÃO DEFINITIVA DAS POLÍTICAS DE ACESSO (RLS)
-- Remove todas as políticas antigas e conflitantes e cria uma única e otimizada por tabela.
-- =================================================================

-- Tabela: public.profiles
DROP POLICY IF EXISTS "Perfis: Usuários autenticados podem ver o próprio perfil" ON public.profiles;
DROP POLICY IF EXISTS "Permitir que usuários aprovados leiam todos os perfis" ON public.profiles;
DROP POLICY IF EXISTS "Permitir que usuários leiam seu próprio perfil" ON public.profiles;
-- Cria uma política única e otimizada para a tabela de perfis
CREATE POLICY "Acesso unificado a perfis" ON public.profiles FOR SELECT USING (
  (public.is_caller_approved()) OR ((select auth.uid()) = id)
);

-- Tabela: public.data_clients
DROP POLICY IF EXISTS "Permitir acesso de leitura para usuários aprovados" ON public.data_clients;
DROP POLICY IF EXISTS "Permitir acesso total para service_role ou aprovados" ON public.data_clients;
CREATE POLICY "Acesso unificado para usuários aprovados" ON public.data_clients FOR ALL USING (public.is_caller_approved()) WITH CHECK (public.is_caller_approved());

-- Tabela: public.data_detailed
DROP POLICY IF EXISTS "Permitir acesso de leitura para usuários aprovados" ON public.data_detailed;
DROP POLICY IF EXISTS "Permitir acesso total para service_role ou aprovados" ON public.data_detailed;
CREATE POLICY "Acesso unificado para usuários aprovados" ON public.data_detailed FOR ALL USING (public.is_caller_approved()) WITH CHECK (public.is_caller_approved());

-- Tabela: public.data_history
DROP POLICY IF EXISTS "Permitir acesso de leitura para usuários aprovados" ON public.data_history;
DROP POLICY IF EXISTS "Permitir acesso total para service_role ou aprovados" ON public.data_history;
CREATE POLICY "Acesso unificado para usuários aprovados" ON public.data_history FOR ALL USING (public.is_caller_approved()) WITH CHECK (public.is_caller_approved());

-- Tabela: public.data_metadata
DROP POLICY IF EXISTS "Permitir acesso de leitura para usuários aprovados" ON public.data_metadata;
DROP POLICY IF EXISTS "Permitir acesso total para service_role ou aprovados" ON public.data_metadata;
CREATE POLICY "Acesso unificado para usuários aprovados" ON public.data_metadata FOR ALL USING (public.is_caller_approved()) WITH CHECK (public.is_caller_approved());

-- Tabela: public.data_orders
DROP POLICY IF EXISTS "Permitir acesso de leitura para usuários aprovados" ON public.data_orders;
DROP POLICY IF EXISTS "Permitir acesso total para service_role ou aprovados" ON public.data_orders;
CREATE POLICY "Acesso unificado para usuários aprovados" ON public.data_orders FOR ALL USING (public.is_caller_approved()) WITH CHECK (public.is_caller_approved());

-- Tabela: public.data_product_details
DROP POLICY IF EXISTS "Permitir acesso de leitura para usuários aprovados" ON public.data_product_details;
DROP POLICY IF EXISTS "Permitir acesso total para service_role ou aprovados" ON public.data_product_details;
CREATE POLICY "Acesso unificado para usuários aprovados" ON public.data_product_details FOR ALL USING (public.is_caller_approved()) WITH CHECK (public.is_caller_approved());


-- =================================================================
-- PARTE 3: LIMPEZA DE ÍNDICES NÃO UTILIZADOS
-- Remove os índices que o Supabase detectou como não utilizados para melhorar a performance de escrita.
-- =================================================================

DROP INDEX IF EXISTS public.idx_stock_product_filial;
DROP INDEX IF EXISTS public.idx_detailed_codusur;
DROP INDEX IF EXISTS public.idx_detailed_nome;
DROP INDEX IF EXISTS public.idx_detailed_codcli;
DROP INDEX IF EXISTS public.idx_detailed_cidade;
DROP INDEX IF EXISTS public.idx_detailed_observacaofor;
DROP INDEX IF EXISTS public.idx_detailed_codfor;
DROP INDEX IF EXISTS public.idx_detailed_posicao;
DROP INDEX IF EXISTS public.idx_detailed_filial;
DROP INDEX IF EXISTS public.idx_history_dtped;
DROP INDEX IF EXISTS public.idx_history_codusur;
DROP INDEX IF EXISTS public.idx_history_nome;
DROP INDEX IF EXISTS public.idx_history_codcli;
DROP INDEX IF EXISTS public.idx_history_cidade;
DROP INDEX IF EXISTS public.idx_history_observacaofor;
DROP INDEX IF EXISTS public.idx_history_codfor;
DROP INDEX IF EXISTS public.idx_history_produto;
DROP INDEX IF EXISTS public.idx_history_filial;
DROP INDEX IF EXISTS public.idx_clients_rca1;
DROP INDEX IF EXISTS public.idx_orders_superv;
DROP INDEX IF EXISTS public.idx_orders_nome;
DROP INDEX IF EXISTS public.idx_orders_codcli;
DROP INDEX IF EXISTS public.idx_orders_posicao;
DROP INDEX IF EXISTS public.idx_orders_codfors_list;
DROP INDEX IF EXISTS public.idx_orders_fornecedores_list;
DROP INDEX IF EXISTS public.idx_product_details_codfor;
DROP INDEX IF EXISTS public.idx_stock_product_code;
DROP INDEX IF EXISTS public.idx_stock_filial;

-- NOTA: O índice 'profiles_id_idx' foi mantido intencionalmente.
-- É normal que ele apareça como não utilizado logo após sua criação.

-- =================================================================
-- FIM DO SCRIPT
-- =================================================================
