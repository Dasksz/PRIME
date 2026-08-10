-- Script de Correção de Segurança para Metas (Goals Distribution)
-- Remove a política antiga permissiva e cria novas políticas segregadas.

-- 1. Remover a política antiga "Acesso Total Unificado" (que permitia escrita para todos os aprovados)
DROP POLICY IF EXISTS "Acesso Total Unificado" ON public.goals_distribution;

-- 2. Criar política de LEITURA (SELECT) para Admins e Usuários Aprovados
-- Todos os aprovados precisam ver as metas para acompanhar o painel.
DROP POLICY IF EXISTS "Goals Read Access" ON public.goals_distribution;
CREATE POLICY "Goals Read Access" ON public.goals_distribution
FOR SELECT
TO authenticated
USING (
  public.is_admin()
  OR
  public.is_approved()
);

-- 3. Criar política de ESCRITA (INSERT, UPDATE, DELETE) APENAS para Admins
-- Apenas administradores podem definir, alterar ou limpar metas.
DROP POLICY IF EXISTS "Goals Write Access" ON public.goals_distribution;
CREATE POLICY "Goals Write Access" ON public.goals_distribution
FOR ALL
TO authenticated
USING (public.is_admin())
WITH CHECK (public.is_admin());
