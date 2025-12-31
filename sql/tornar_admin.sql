-- Script para tornar um usuário Administrador (necessário para Upload de Arquivos)

-- INSTRUÇÕES:
-- 1. Abra o Editor SQL no painel do Supabase.
-- 2. Substitua 'seu_email@exemplo.com' pelo seu email de login abaixo.
-- 3. Execute o script.

UPDATE public.profiles
SET role = 'adm', status = 'aprovado'
WHERE email = 'seu_email@exemplo.com';

-- Verifica se a atualização funcionou (retorna o usuário atualizado)
SELECT * FROM public.profiles WHERE email = 'seu_email@exemplo.com';
