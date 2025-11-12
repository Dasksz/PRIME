-- =================================================================
-- SCRIPT DE CORREÇÃO FINAL PARA POLÍTICAS DE RLS (ROW LEVEL SECURITY)
-- VERSÃO: V10.0
-- OBJETIVO: Otimizar as políticas (V9.0) para performance,
-- resolvendo o aviso 'auth_rls_initplan' do linter.
-- =================================================================
-- ETAPA 1: FUNÇÃO AUXILIAR OTIMIZADA
-- Mudança: auth.uid() -> (select auth.uid())
-- Isso faz com que o ID do usuário seja verificado apenas UMA VEZ por consulta.
create or replace function public.is_caller_approved () RETURNS boolean LANGUAGE sql STABLE -- Otimiza a execução da função
SECURITY DEFINER
-- Define um search_path estático para segurança e performance.
set
  search_path = public as $$
  SELECT EXISTS (
    SELECT 1 FROM profiles 
    WHERE id = (select auth.uid()) AND status = 'aprovado'
  );
$$;

-- =================================================================
-- ETAPA 2: APLICAR POLÍTICAS OTIMIZADAS PARA CADA TABELA DE DADOS
-- =================================================================
-- Padrão aplicado:
-- Mudança: auth.role() -> (select auth.role())
-- Isso faz com que a role do usuário seja verificada apenas UMA VEZ por consulta.
-- Tabela: data_detailed
alter table public.data_detailed ENABLE row LEVEL SECURITY;

drop policy IF exists "Permitir acesso total para service_role ou aprovados" on public.data_detailed;

create policy "Permitir acesso total para service_role ou aprovados" on public.data_detailed for all using (
  (
    (
      select
        auth.role ()
    ) = 'service_role'
  )
  or (public.is_caller_approved ())
)
with
  check (
    (
      (
        select
          auth.role ()
      ) = 'service_role'
    )
    or (public.is_caller_approved ())
  );

-- Tabela: data_history
alter table public.data_history ENABLE row LEVEL SECURITY;

drop policy IF exists "Permitir acesso total para service_role ou aprovados" on public.data_history;

create policy "Permitir acesso total para service_role ou aprovados" on public.data_history for all using (
  (
    (
      select
        auth.role ()
    ) = 'service_role'
  )
  or (public.is_caller_approved ())
)
with
  check (
    (
      (
        select
          auth.role ()
      ) = 'service_role'
    )
    or (public.is_caller_approved ())
  );

-- Tabela: data_clients
alter table public.data_clients ENABLE row LEVEL SECURITY;

drop policy IF exists "Permitir acesso total para service_role ou aprovados" on public.data_clients;

create policy "Permitir acesso total para service_role ou aprovados" on public.data_clients for all using (
  (
    (
      select
        auth.role ()
    ) = 'service_role'
  )
  or (public.is_caller_approved ())
)
with
  check (
    (
      (
        select
          auth.role ()
      ) = 'service_role'
    )
    or (public.is_caller_approved ())
  );

-- Tabela: data_orders
alter table public.data_orders ENABLE row LEVEL SECURITY;

drop policy IF exists "Permitir acesso total para service_role ou aprovados" on public.data_orders;

create policy "Permitir acesso total para service_role ou aprovados" on public.data_orders for all using (
  (
    (
      select
        auth.role ()
    ) = 'service_role'
  )
  or (public.is_caller_approved ())
)
with
  check (
    (
      (
        select
          auth.role ()
      ) = 'service_role'
    )
    or (public.is_caller_approved ())
  );

-- Tabela: data_product_details
alter table public.data_product_details ENABLE row LEVEL SECURITY;

drop policy IF exists "Permitir acesso total para service_role ou aprovados" on public.data_product_details;

create policy "Permitir acesso total para service_role ou aprovados" on public.data_product_details for all using (
  (
    (
      select
        auth.role ()
    ) = 'service_role'
  )
  or (public.is_caller_approved ())
)
with
  check (
    (
      (
        select
          auth.role ()
      ) = 'service_role'
    )
    or (public.is_caller_approved ())
  );

-- Tabela: data_active_products
alter table public.data_active_products ENABLE row LEVEL SECURITY;

drop policy IF exists "Permitir acesso total para service_role ou aprovados" on public.data_active_products;

create policy "Permitir acesso total para service_role ou aprovados" on public.data_active_products for all using (
  (
    (
      select
        auth.role ()
    ) = 'service_role'
  )
  or (public.is_caller_approved ())
)
with
  check (
    (
      (
        select
          auth.role ()
      ) = 'service_role'
    )
    or (public.is_caller_approved ())
  );

-- Tabela: data_stock
alter table public.data_stock ENABLE row LEVEL SECURITY;

drop policy IF exists "Permitir acesso total para service_role ou aprovados" on public.data_stock;

create policy "Permitir acesso total para service_role ou aprovados" on public.data_stock for all using (
  (
    (
      select
        auth.role ()
    ) = 'service_role'
  )
  or (public.is_caller_approved ())
)
with
  check (
    (
      (
        select
          auth.role ()
      ) = 'service_role'
    )
    or (public.is_caller_approved ())
  );

-- Tabela: data_innovations
alter table public.data_innovations ENABLE row LEVEL SECURITY;

drop policy IF exists "Permitir acesso total para service_role ou aprovados" on public.data_innovations;

create policy "Permitir acesso total para service_role ou aprovados" on public.data_innovations for all using (
  (
    (
      select
        auth.role ()
    ) = 'service_role'
  )
  or (public.is_caller_approved ())
)
with
  check (
    (
      (
        select
          auth.role ()
      ) = 'service_role'
    )
    or (public.is_caller_approved ())
  );

-- Tabela: data_metadata
alter table public.data_metadata ENABLE row LEVEL SECURITY;

drop policy IF exists "Permitir acesso total para service_role ou aprovados" on public.data_metadata;

create policy "Permitir acesso total para service_role ou aprovados" on public.data_metadata for all using (
  (
    (
      select
        auth.role ()
    ) = 'service_role'
  )
  or (public.is_caller_approved ())
)
with
  check (
    (
      (
        select
          auth.role ()
      ) = 'service_role'
    )
    or (public.is_caller_approved ())
  );

-- =================================================================
-- ETAPA FINAL: Forçar o Supabase a recarregar o esquema
-- =================================================================
notify pgrst,
'reload schema';
