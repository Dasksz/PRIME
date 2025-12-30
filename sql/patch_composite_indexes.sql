-- Add Composite Indexes to optimize "WHERE codusur IN (...) ORDER BY id"
-- This is crucial for fixing timeouts when Explicit Filtering is used in the frontend.

create index if not exists idx_data_history_codusur_id on public.data_history (codusur, id);
create index if not exists idx_data_detailed_codusur_id on public.data_detailed (codusur, id);
create index if not exists idx_data_orders_codusur_id on public.data_orders (codusur, id);
