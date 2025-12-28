-- Função para limpar tabelas rapidamente usando TRUNCATE
-- Isso evita o erro "canceling statement due to statement timeout" ao deletar muitas linhas
CREATE OR REPLACE FUNCTION truncate_table(table_name text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  -- Verifica se a tabela existe para evitar erro
  IF EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = truncate_table.table_name) THEN
    EXECUTE format('TRUNCATE TABLE public.%I', table_name);
  ELSE
    RAISE EXCEPTION 'Table % does not exist', table_name;
  END IF;
END;
$$;
