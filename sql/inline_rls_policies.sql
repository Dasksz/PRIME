-- Optimize RLS Policies by Inlining Logic (Avoids PL/PGSQL function overhead per row)
-- This is critical for large table performance (data_history, data_detailed)

-- 1. data_detailed
drop policy if exists "Acesso leitura seguro" on public.data_detailed;
create policy "Acesso leitura seguro" on public.data_detailed for select
using (
  auth.role() = 'authenticated'
  and exists (
    select 1 from public.profiles
    where id = auth.uid()
    and status = 'aprovado'
    and (role = 'adm' or rcas @> array[codusur])
  )
);

-- 2. data_history
drop policy if exists "Acesso leitura seguro" on public.data_history;
create policy "Acesso leitura seguro" on public.data_history for select
using (
  auth.role() = 'authenticated'
  and exists (
    select 1 from public.profiles
    where id = auth.uid()
    and status = 'aprovado'
    and (role = 'adm' or rcas @> array[codusur])
  )
);

-- 3. data_orders
drop policy if exists "Acesso leitura seguro" on public.data_orders;
create policy "Acesso leitura seguro" on public.data_orders for select
using (
  auth.role() = 'authenticated'
  and exists (
    select 1 from public.profiles
    where id = auth.uid()
    and status = 'aprovado'
    and (role = 'adm' or rcas @> array[codusur])
  )
);

-- 4. data_clients
drop policy if exists "Acesso leitura seguro" on public.data_clients;
create policy "Acesso leitura seguro" on public.data_clients for select
using (
  auth.role() = 'authenticated'
  and exists (
    select 1 from public.profiles
    where id = auth.uid()
    and status = 'aprovado'
    and (role = 'adm' or rcas && public.data_clients.rcas)
  )
);
