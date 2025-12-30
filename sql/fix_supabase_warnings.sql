-- Correção de Avisos de Performance e Segurança do Supabase

-- 1. Remover índices duplicados
-- O aviso indicava que idx_data_detailed_codusur e idx_detailed_codusur eram idênticos.
DROP INDEX IF EXISTS public.idx_detailed_codusur;
-- O aviso indicava que idx_data_history_codusur e idx_history_codusur eram idênticos.
DROP INDEX IF EXISTS public.idx_history_codusur;

-- 2. Função auxiliar para verificar admin
-- Necessária para as políticas unificadas abaixo.
CREATE OR REPLACE FUNCTION public.is_admin()
RETURNS boolean AS $$
BEGIN
  IF auth.role() = 'service_role' THEN RETURN true; END IF;
  RETURN EXISTS (SELECT 1 FROM public.profiles WHERE id = auth.uid() AND role = 'admin');
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 3. Corrigir políticas para data_product_details
-- O problema era "Multiple Permissive Policies" para SELECT (Admin e Aprovado tinham políticas separadas que se sobrepunham).
DROP POLICY IF EXISTS "Acesso escrita admin" ON public.data_product_details;
DROP POLICY IF EXISTS "Acesso leitura seguro" ON public.data_product_details;
DROP POLICY IF EXISTS "Acesso leitura aprovados" ON public.data_product_details;
DROP POLICY IF EXISTS "Enable read access for all users" ON public.data_product_details;

-- Política de Leitura Unificada (Admin OU Aprovado)
CREATE POLICY "Acesso Leitura Unificado" ON public.data_product_details
FOR SELECT TO authenticated
USING (public.is_admin() OR public.is_approved());

-- Políticas de Escrita para Admin (Separadas por ação para evitar sobreposição com SELECT se usasse FOR ALL)
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


-- 4. Corrigir políticas para data_stock
DROP POLICY IF EXISTS "Acesso escrita admin" ON public.data_stock;
DROP POLICY IF EXISTS "Acesso leitura seguro" ON public.data_stock;
DROP POLICY IF EXISTS "Acesso leitura aprovados" ON public.data_stock;
DROP POLICY IF EXISTS "Enable read access for all users" ON public.data_stock;

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


-- 5. Corrigir políticas para goals_distribution
-- O problema era ter uma política "FOR SELECT" e outra "FOR ALL" (que inclui SELECT) para os mesmos usuários.
DROP POLICY IF EXISTS "Acesso escrita admin" ON public.goals_distribution;
DROP POLICY IF EXISTS "Acesso leitura seguro" ON public.goals_distribution;
DROP POLICY IF EXISTS "Acesso leitura aprovados" ON public.goals_distribution;
DROP POLICY IF EXISTS "Acesso escrita aprovados" ON public.goals_distribution;
DROP POLICY IF EXISTS "Enable read access for all users" ON public.goals_distribution;
DROP POLICY IF EXISTS "Enable insert/update for goals" ON public.goals_distribution;

-- Política Única para Leitura e Escrita (Aprovados e Admins)
-- Se is_approved() retornar true, permite tudo. Se for admin (e não aprovado), permite tudo.
CREATE POLICY "Acesso Total Aprovados e Admin" ON public.goals_distribution
FOR ALL TO authenticated
USING (public.is_admin() OR public.is_approved())
WITH CHECK (public.is_admin() OR public.is_approved());
