-- =================================================================
-- SCRIPT MESTRE DE CONFIGURAÇÃO DO SUPABASE (UNIFICADO)
-- VERSÃO: V6.1 (COM OTIMIZAÇÃO DE PERFORMANCE RLS)
-- =================================================================

-- =================================================================
-- ETAPA 1: Apagar tudo (Tabelas, Função, Trigger)
-- =================================================================
-- Apaga as 9 tabelas de dados
drop table if exists data_detailed;
drop table if exists data_history;
drop table if exists data_clients;
drop table if exists data_orders;
drop table if exists data_products;
drop table if exists data_product_details;
drop table if exists data_stock;
drop table if exists data_innovations;
drop table if exists data_metadata;
drop table if exists data_active_products;

-- Apaga a tabela de perfis (do nosso sistema de aprovação)
drop table if exists public.profiles;

-- Apaga a função do trigger (para garantir que seja recriada)
-- Adicionado 'cascade' para apagar o trigger que depende dela
drop function if exists public.handle_new_user cascade;

-- =================================================================
-- ETAPA 2: Criar as 10 tabelas (9 de dados + 1 de perfis)
-- =================================================================
-- Tabela 1: Vendas Detalhadas (detailed)
create table data_detailed (
  id bigserial primary key,
  pedido text,
  codusur text,
  codcli text,
  produto text,
  codfor text,
  filial text,
  codsupervisor text,
  nome text,
  superv text,
  descricao text,
  fornecedor text,
  observacaofor text,
  cliente_nome text,
  cidade text,
  bairro text,
  posicao text,
  tipovenda text,
  qtvenda integer,
  vlvenda numeric,
  vlbonific numeric,
  totpesoliq float8,
  estoqueunit float8,
  qtvenda_embalagem_master float8,
  dtped timestamptz,
  dtsaida timestamptz
);

-- Tabela 2: Histórico de Vendas (history)
create table data_history (
  id bigserial primary key,
  pedido text,
  codusur text,
  codcli text,
  produto text,
  codfor text,
  filial text,
  codsupervisor text,
  nome text,
  superv text,
  descricao text,
  fornecedor text,
  observacaofor text,
  cliente_nome text,
  cidade text,
  bairro text,
  posicao text,
  tipovenda text,
  qtvenda integer,
  vlvenda numeric,
  vlbonific numeric,
  totpesoliq float8,
  estoqueunit float8,
  qtvenda_embalagem_master float8,
  dtped timestamptz,
  dtsaida timestamptz
);

-- Tabela 3: Clientes (clients)
create table data_clients (
  codigo_cliente text primary key,
  rcas text[],
  rca1 text,
  rca2 text,
  cidade text,
  nomecliente text,
  bairro text,
  razaosocial text,
  fantasia text,
  cnpj_cpf text,
  endereco text,
  numero text,
  cep text,
  telefone text,
  email text,
  ramo text,
  ultimacompra text,
  datacadastro text,
  bloqueio text,
  inscricaoestadual text
);

-- Tabela 4: Pedidos Agregados (byOrder)
create table data_orders (
  id bigserial primary key,
  pedido text,
  codusur text,
  codcli text,
  produto text,
  codfor text,
  filial text,
  codsupervisor text,
  nome text,
  superv text,
  descricao text,
  fornecedor text,
  observacaofor text,
  cliente_nome text,
  cidade text,
  bairro text,
  posicao text,
  tipovenda text,
  qtvenda integer,
  vlvenda numeric,
  vlbonific numeric,
  totpesoliq float8,
  estoqueunit float8,
  qtvenda_embalagem_master float8,
  dtped timestamptz,
  dtsaida timestamptz,
  fornecedores_list text[],
  fornecedores_str text,
  codfors_list text[]
);

-- Tabela 5: Detalhes dos Produtos (productDetails)
create table data_product_details (
  code text primary key,
  descricao text,
  fornecedor text,
  codfor text,
  dtcadastro text
);

-- Tabela 6: Códigos de Produtos Ativos (activeProductCodes)
create table data_active_products (code text primary key);

-- Tabela 7: Estoque (stockMap)
create table data_stock (
  id bigserial primary key,
  product_code text,
  filial text,
  stock_qty numeric
);

-- Tabela 8: Inovações (innovationsMonth)
create table data_innovations (
  id bigserial primary key,
  inovacoes text,
  codigo text,
  produto text
);

-- Tabela 9: Metadados (metadata)
create table data_metadata (key text primary key, value text);

-- Tabela 10: PROFILES (A nossa lista de aprovação)
create table public.profiles (
  id uuid primary key references auth.users (id),
  email text,
  status text not null default 'pendente'
);

-- =================================================================
-- ETAPA 3: Criar a Automação (Função e Trigger)
-- =================================================================
create function public.handle_new_user () returns trigger as $$
begin
  insert into public.profiles (id, email, status)
  values (
    new.id,
    new.email,
    'pendente'
  );
  return new;
end;
$$ language plpgsql
security definer
-- CORREÇÃO DE SEGURANÇA APLICADA AQUI:
SET search_path = public, pg_temp;

-- Cria o trigger (gatilho)
create trigger on_auth_user_created
after insert on auth.users for each row
execute procedure public.handle_new_user ();

-- =================================================================
-- ETAPA 4: Habilitar RLS e Definir Políticas de Segurança
-- =================================================================
-- Regra para a Tabela 'profiles' (Permite que o usuário LEIA o próprio status)
alter table public.profiles ENABLE row LEVEL SECURITY;

drop policy IF exists "Usuários podem ver o próprio perfil" on public.profiles;

create policy "Usuários podem ver o próprio perfil" on public.profiles for
select
  using (id = (select auth.uid()));

-- -----------------------------------------------------------------
-- Regras para as 9 Tabelas de Dados (Apenas usuários 'aprovado')
-- -----------------------------------------------------------------
-- data_detailed
alter table data_detailed ENABLE row LEVEL SECURITY;
drop policy IF exists "Permitir leitura pública detailed" on data_detailed;
drop policy IF exists "Apenas usuários aprovados podem acessar" on data_detailed;
create policy "Apenas usuários aprovados podem acessar" on data_detailed for all using (
  exists (
    select
      1
    from
      public.profiles
    where
      profiles.id = (select auth.uid())
      and profiles.status = 'aprovado'
  )
);
drop policy IF exists "Permitir apagar (delete) para service_role" on data_detailed;
create policy "Permitir apagar (delete) para service_role" on data_detailed for DELETE to service_role using (true);

-- data_history
alter table data_history ENABLE row LEVEL SECURITY;
drop policy IF exists "Permititir leitura pública history" on data_history;
drop policy IF exists "Apenas usuários aprovados podem acessar" on data_history;
create policy "Apenas usuários aprovados podem acessar" on data_history for all using (
  exists (
    select
      1
    from
      public.profiles
    where
      profiles.id = (select auth.uid())
      and profiles.status = 'aprovado'
  )
);
drop policy IF exists "Permitir apagar (delete) para service_role" on data_history;
create policy "Permitir apagar (delete) para service_role" on data_history for DELETE to service_role using (true);

-- data_clients
alter table data_clients ENABLE row LEVEL SECURITY;
drop policy IF exists "Permitir leitura pública clients" on data_clients;
drop policy IF exists "Apenas usuários aprovados podem acessar" on data_clients;
create policy "Apenas usuários aprovados podem acessar" on data_clients for all using (
  exists (
    select
      1
    from
      public.profiles
    where
      profiles.id = (select auth.uid())
      and profiles.status = 'aprovado'
  )
);
drop policy IF exists "Permitir apagar (delete) para service_role" on data_clients;
create policy "Permitir apagar (delete) para service_role" on data_clients for DELETE to service_role using (true);

-- data_orders
alter table data_orders ENABLE row LEVEL SECURITY;
drop policy IF exists "Permitir leitura pública orders" on data_orders;
drop policy IF exists "Apenas usuários aprovados podem acessar" on data_orders;
create policy "Apenas usuários aprovados podem acessar" on data_orders for all using (
  exists (
    select
      1
    from
      public.profiles
    where
      profiles.id = (select auth.uid())
      and profiles.status = 'aprovado'
  )
);
drop policy IF exists "Permitir apagar (delete) para service_role" on data_orders;
create policy "Permitir apagar (delete) para service_role" on data_orders for DELETE to service_role using (true);

-- data_product_details
alter table data_product_details ENABLE row LEVEL SECURITY;
drop policy IF exists "Permitir leitura pública product_details" on data_product_details;
drop policy IF exists "Apenas usuários aprovados podem acessar" on data_product_details;
create policy "Apenas usuários aprovados podem acessar" on data_product_details for all using (
  exists (
    select
      1
    from
      public.profiles
    where
      profiles.id = (select auth.uid())
      and profiles.status = 'aprovado'
  )
);
drop policy IF exists "Permitir apagar (delete) para service_role" on data_product_details;
create policy "Permitir apagar (delete) para service_role" on data_product_details for DELETE to service_role using (true);

-- data_active_products
alter table data_active_products ENABLE row LEVEL SECURITY;
drop policy IF exists "Permitir leitura pública active_products" on data_active_products;
drop policy IF exists "Apenas usuários aprovados podem acessar" on data_active_products;
create policy "Apenas usuários aprovados podem acessar" on data_active_products for all using (
  exists (
    select
      1
    from
      public.profiles
    where
      profiles.id = (select auth.uid())
      and profiles.status = 'aprovado'
  )
);
drop policy IF exists "Permitir apagar (delete) para service_role" on data_active_products;
create policy "Permitir apagar (delete) para service_role" on data_active_products for DELETE to service_role using (true);

-- data_stock
alter table data_stock ENABLE row LEVEL SECURITY;
drop policy IF exists "Permitir leitura pública stock" on data_stock;
drop policy IF exists "Apenas usuários aprovados podem acessar" on data_stock;
create policy "Apenas usuários aprovados podem acessar" on data_stock for all using (
  exists (
    select
      1
    from
      public.profiles
    where
      profiles.id = (select auth.uid())
      and profiles.status = 'aprovado'
  )
);
drop policy IF exists "Permitir apagar (delete) para service_role" on data_stock;
create policy "Permitir apagar (delete) para service_role" on data_stock for DELETE to service_role using (true);

-- data_innovations
alter table data_innovations ENABLE row LEVEL SECURITY;
drop policy IF exists "Permitir leitura pública innovations" on data_innovations;
drop policy IF exists "Apenas usuários aprovados podem acessar" on data_innovations;
create policy "Apenas usuários aprovados podem acessar" on data_innovations for all using (
  exists (
    select
      1
    from
      public.profiles
    where
      profiles.id = (select auth.uid())
      and profiles.status = 'aprovado'
  )
);
drop policy IF exists "Permitir apagar (delete) para service_role" on data_innovations;
create policy "Permitir apagar (delete) para service_role" on data_innovations for DELETE to service_role using (true);

-- data_metadata
alter table data_metadata ENABLE row LEVEL SECURITY;
drop policy IF exists "Permitir leitura pública metadata" on data_metadata;
drop policy IF exists "Apenas usuários aprovados podem acessar" on data_metadata;
create policy "Apenas usuários aprovados podem acessar" on data_metadata for all using (
  exists (
    select
      1
    from
      public.profiles
    where
      profiles.id = (select auth.uid())
      and profiles.status = 'aprovado'
  )
);
drop policy IF exists "Permitir apagar (delete) para service_role" on data_metadata;
create policy "Permitir apagar (delete) para service_role" on data_metadata for DELETE to service_role using (true);

-- =================================================================
-- ETAPA 5: Criar índices para otimização
-- =================================================================
create index IF not exists idx_stock_product_filial on data_stock (product_code, filial);

-- =================================================================
-- ETAPA 6: Forçar o Supabase a reler o esquema
-- =================================================================
notify pgrst, 'reload schema';