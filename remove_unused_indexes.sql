-- Remove Unused Indexes

-- This file contains SQL commands to remove indexes that Supabase linter identified as unused.
-- NOTE: Only run this if you are sure these indexes are not needed for future queries (e.g., filtering/sorting).

DROP INDEX IF EXISTS public.idx_clients_codigo;
DROP INDEX IF EXISTS public.idx_clients_rca1;
DROP INDEX IF EXISTS public.idx_stock_product;
DROP INDEX IF EXISTS public.idx_detailed_codcli;
DROP INDEX IF EXISTS public.idx_detailed_produto;
DROP INDEX IF EXISTS public.idx_history_codcli;
DROP INDEX IF EXISTS public.idx_history_codusur;
DROP INDEX IF EXISTS public.idx_goals_distribution_updated_by;
