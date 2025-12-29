-- Function to truncate table (for Admin Upload)
-- Allows clearing tables securely if the user is authenticated (RLS policies will still apply if not using truncate, but here we use dynamic SQL with a whitelist)
create or replace function public.truncate_table(table_name text)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  -- Check if user is approved (same logic as RLS)
  if not public.is_approved() then
    raise exception 'Access denied: User not approved.';
  end if;

  -- Validate table name to prevent SQL injection (allow-list)
  if table_name not in ('data_detailed', 'data_history', 'data_orders', 'data_clients', 'data_stock', 'data_innovations', 'data_product_details', 'data_active_products', 'data_metadata', 'goals_distribution') then
    raise exception 'Invalid table name';
  end if;

  -- Use TRUNCATE for speed and complete clearing
  execute format('truncate table public.%I', table_name);
end;
$$;

-- Function placeholder for get_initial_dashboard_data
-- This function appears to be missing but requested by some client versions.
-- Returning empty JSON prevents 404 errors.
create or replace function public.get_initial_dashboard_data()
returns json
language plpgsql
security definer
set search_path = public
as $$
begin
  return '{}'::json;
end;
$$;
