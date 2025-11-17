-- Este script remove os índices que foram identificados como não utilizados pelo linter de performance do Supabase.
-- A remoção de índices desnecessários pode melhorar o desempenho das operações de escrita (INSERT, UPDATE, DELETE).
-- NOTA: Os índices 'idx_data_detailed_dtped', 'idx_data_history_dtped', 'idx_detailed_observacaofor', 'idx_detailed_codfor',
-- e 'idx_detailed_posicao' foram removidos deste script porque são criados no script de otimização e são necessários para a performance.

DROP INDEX IF EXISTS public.idx_stock_product_filial;
DROP INDEX IF EXISTS public.idx_detailed_codusur;
DROP INDEX IF EXISTS public.idx_detailed_nome;
DROP INDEX IF EXISTS public.idx_detailed_codcli;
DROP INDEX IF EXISTS public.idx_detailed_cidade;
DROP INDEX IF EXISTS public.idx_detailed_filial;
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
DROP INDEX IF EXISTS public.profiles_id_idx;
DROP INDEX IF EXISTS public.idx_orders_nome;
DROP INDEX IF EXISTS public.idx_orders_codcli;
DROP INDEX IF EXISTS public.idx_orders_posicao;
DROP INDEX IF EXISTS public.idx_orders_codfors_list;
DROP INDEX IF EXISTS public.idx_orders_fornecedores_list;
DROP INDEX IF EXISTS public.idx_product_details_codfor;
DROP INDEX IF EXISTS public.idx_stock_product_code;
DROP INDEX IF EXISTS public.idx_stock_filial;
