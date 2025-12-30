-- Enable UUID extension
create extension if not exists "uuid-ossp";

-- 1. Tabela de Vendas Detalhadas (Mês Atual)
create table if not exists public.data_detailed (
  id uuid default uuid_generate_v4 () primary key,
  pedido text,
  nome text, -- Vendedor
  superv text, -- Supervisor
  produto text,
  descricao text,
  fornecedor text,
  observacaofor text, -- Pasta
  codfor text,
  codusur text,
  codcli text,
  qtvenda numeric,
  codsupervisor text,
  vlvenda numeric,
  vlbonific numeric,
  totpesoliq numeric,
  dtped timestamp with time zone,
  dtsaida timestamp with time zone,
  posicao text,
  estoqueunit numeric,
  qtvenda_embalagem_master numeric,
  tipovenda text,
  filial text,
  cliente_nome text, -- Otimização: Desnormalizado para reduzir lookups no front
  cidade text,
  bairro text
);

alter table public.data_detailed add column if not exists observacaofor text;

-- 2. Tabela de Histórico de Vendas (Trimestre)
create table if not exists public.data_history (
  id uuid default uuid_generate_v4 () primary key,
  pedido text,
  nome text,
  superv text,
  produto text,
  descricao text,
  fornecedor text,
  observacaofor text,
  codfor text,
  codusur text,
  codcli text,
  qtvenda numeric,
  codsupervisor text,
  vlvenda numeric,
  vlbonific numeric,
  totpesoliq numeric,
  dtped timestamp with time zone,
  dtsaida timestamp with time zone,
  posicao text,
  estoqueunit numeric,
  qtvenda_embalagem_master numeric,
  tipovenda text,
  filial text
);

alter table public.data_history add column if not exists observacaofor text;

-- 3. Tabela de Clientes
create table if not exists public.data_clients (
  id uuid default uuid_generate_v4 () primary key,
  codigo_cliente text unique,
  rca1 text,
  rca2 text,
  rcas text[], -- Array de RCAs
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
  ultimacompra timestamp with time zone,
  datacadastro timestamp with time zone,
  bloqueio text,
  inscricaoestadual text
);

-- 4. Tabela de Pedidos Agregados
create table if not exists public.data_orders (
  id uuid default uuid_generate_v4 () primary key,
  pedido text unique,
  codcli text,
  cliente_nome text,
  cidade text,
  nome text, -- Vendedor
  superv text, -- Supervisor
  fornecedores_str text,
  dtped timestamp with time zone,
  dtsaida timestamp with time zone,
  posicao text,
  vlvenda numeric,
  totpesoliq numeric,
  filial text,
  tipovenda text,
  fornecedores_list text[],
  codfors_list text[],
  codusur text
);

alter table public.data_orders add column if not exists tipovenda text;
alter table public.data_orders add column if not exists fornecedores_list text[];
alter table public.data_orders add column if not exists codfors_list text[];
alter table public.data_orders add column if not exists codusur text;

-- 5. Tabela de Detalhes de Produtos
create table if not exists public.data_product_details (
  code text primary key,
  descricao text,
  fornecedor text,
  codfor text,
  dtcadastro timestamp with time zone,
  pasta text
);

alter table public.data_product_details add column if not exists pasta text;

-- 6. Tabela de Produtos Ativos
create table if not exists public.data_active_products (code text primary key);

-- 7. Tabela de Estoque
create table if not exists public.data_stock (
  id uuid default uuid_generate_v4 () primary key,
  product_code text,
  filial text,
  stock_qty numeric
);

-- 8. Tabela de Inovações
create table if not exists public.data_innovations (
  id uuid default uuid_generate_v4 () primary key,
  codigo text,
  produto text,
  inovacoes text
);

-- 9. Tabela de Metadados
create table if not exists public.data_metadata (key text primary key, value text);

-- 10. Tabela para Salvar Metas
create table if not exists public.goals_distribution (
  id uuid default uuid_generate_v4 () primary key,
  month_key text not null,
  supplier text not null,
  brand text default 'GENERAL',
  goals_data jsonb not null,
  updated_at timestamp with time zone default now(),
  updated_by text
);

create unique index if not exists idx_goals_unique on public.goals_distribution (month_key, supplier, brand);

-- 11. Tabela de Perfis de Usuário
create table if not exists public.profiles (
  id uuid references auth.users on delete cascade not null primary key,
  email text,
  status text default 'pendente', -- pendente, aprovado, bloqueado
  role text default 'user',
  rcas text[], -- Array de RCAs permitidos para o usuário
  created_at timestamp with time zone default now(),
  updated_at timestamp with time zone default now()
);

alter table public.profiles add column if not exists rcas text[];

-- RLS Enable
alter table public.data_detailed enable row level security;
alter table public.data_history enable row level security;
alter table public.data_clients enable row level security;
alter table public.data_orders enable row level security;
alter table public.data_product_details enable row level security;
alter table public.data_active_products enable row level security;
alter table public.data_stock enable row level security;
alter table public.data_innovations enable row level security;
alter table public.data_metadata enable row level security;
alter table public.goals_distribution enable row level security;
alter table public.profiles enable row level security;

-- --- POLÍTICAS DE SEGURANÇA (RLS) ---
-- Apenas usuários autenticados E com status 'aprovado' na tabela profiles podem ler os dados.

-- Função auxiliar para verificar se o usuário está aprovado
create or replace function public.is_approved()
returns boolean as $$
begin
  -- Allow service_role to bypass approval check
  if auth.role() = 'service_role' then
    return true;
  end if;

  return exists (
    select 1 from public.profiles
    where id = auth.uid()
    and status = 'aprovado'
  );
end;
$$ language plpgsql security definer;

-- Função auxiliar para verificar se o usuário é ADMIN
create or replace function public.is_admin()
returns boolean as $$
begin
  -- Allow service_role to bypass check
  if auth.role() = 'service_role' then
    return true;
  end if;

  return exists (
    select 1 from public.profiles
    where id = auth.uid()
    and status = 'aprovado'
    and role = 'adm'
  );
end;
$$ language plpgsql security definer;

-- Função auxiliar para obter os RCAs do usuário
create or replace function public.get_user_rcas()
returns text[] as $$
declare
  user_rcas text[];
begin
  select rcas into user_rcas
  from public.profiles
  where id = auth.uid();
  
  return coalesce(user_rcas, array[]::text[]);
end;
$$ language plpgsql security definer;

-- Aplicando políticas

-- Data Detailed
drop policy if exists "Enable read access for all users" on public.data_detailed;
drop policy if exists "Acesso leitura aprovados" on public.data_detailed;
create policy "Acesso leitura seguro" on public.data_detailed for select
using (
  auth.role() = 'authenticated' 
  and public.is_approved() 
  and (
    public.is_admin() 
    or 
    (codusur = any(public.get_user_rcas()))
  )
);

drop policy if exists "Acesso escrita admin" on public.data_detailed;
create policy "Acesso escrita admin" on public.data_detailed for all
using (auth.role() = 'authenticated' and public.is_admin())
with check (auth.role() = 'authenticated' and public.is_admin());

-- Data History
drop policy if exists "Enable read access for all users" on public.data_history;
drop policy if exists "Acesso leitura aprovados" on public.data_history;
create policy "Acesso leitura seguro" on public.data_history for select
using (
  auth.role() = 'authenticated' 
  and public.is_approved() 
  and (
    public.is_admin() 
    or 
    (codusur = any(public.get_user_rcas()))
  )
);

drop policy if exists "Acesso escrita admin" on public.data_history;
create policy "Acesso escrita admin" on public.data_history for all
using (auth.role() = 'authenticated' and public.is_admin())
with check (auth.role() = 'authenticated' and public.is_admin());

-- Data Clients
drop policy if exists "Enable read access for all users" on public.data_clients;
drop policy if exists "Acesso leitura aprovados" on public.data_clients;
create policy "Acesso leitura seguro" on public.data_clients for select
using (
  auth.role() = 'authenticated' 
  and public.is_approved() 
  and (
    public.is_admin() 
    or 
    (rcas && public.get_user_rcas()) -- Verifica interseção de arrays
  )
);

drop policy if exists "Acesso escrita admin" on public.data_clients;
create policy "Acesso escrita admin" on public.data_clients for all
using (auth.role() = 'authenticated' and public.is_admin())
with check (auth.role() = 'authenticated' and public.is_admin());

-- Data Orders
drop policy if exists "Enable read access for all users" on public.data_orders;
drop policy if exists "Acesso leitura aprovados" on public.data_orders;
create policy "Acesso leitura seguro" on public.data_orders for select
using (
  auth.role() = 'authenticated' 
  and public.is_approved() 
  and (
    public.is_admin() 
    or 
    (codusur = any(public.get_user_rcas()))
  )
);

drop policy if exists "Acesso escrita admin" on public.data_orders;
create policy "Acesso escrita admin" on public.data_orders for all
using (auth.role() = 'authenticated' and public.is_admin())
with check (auth.role() = 'authenticated' and public.is_admin());

-- Product Details (Público para aprovados, não sensível por usuário)
drop policy if exists "Enable read access for all users" on public.data_product_details;
drop policy if exists "Acesso leitura aprovados" on public.data_product_details;
create policy "Acesso leitura seguro" on public.data_product_details for select
using (auth.role() = 'authenticated' and public.is_approved());

drop policy if exists "Acesso escrita admin" on public.data_product_details;
create policy "Acesso escrita admin" on public.data_product_details for all
using (auth.role() = 'authenticated' and public.is_admin())
with check (auth.role() = 'authenticated' and public.is_admin());

-- Active Products
drop policy if exists "Enable read access for all users" on public.data_active_products;
drop policy if exists "Acesso leitura aprovados" on public.data_active_products;
create policy "Acesso leitura seguro" on public.data_active_products for select
using (auth.role() = 'authenticated' and public.is_approved());

drop policy if exists "Acesso escrita admin" on public.data_active_products;
create policy "Acesso escrita admin" on public.data_active_products for all
using (auth.role() = 'authenticated' and public.is_admin())
with check (auth.role() = 'authenticated' and public.is_admin());

-- Stock (Pode ser sensível, mas geralmente é geral. Se precisar filtrar, estoque por filial. Por enquanto deixo geral para aprovados)
drop policy if exists "Enable read access for all users" on public.data_stock;
drop policy if exists "Acesso leitura aprovados" on public.data_stock;
create policy "Acesso leitura seguro" on public.data_stock for select
using (auth.role() = 'authenticated' and public.is_approved());

drop policy if exists "Acesso escrita admin" on public.data_stock;
create policy "Acesso escrita admin" on public.data_stock for all
using (auth.role() = 'authenticated' and public.is_admin())
with check (auth.role() = 'authenticated' and public.is_admin());

-- Innovations
drop policy if exists "Enable read access for all users" on public.data_innovations;
drop policy if exists "Acesso leitura aprovados" on public.data_innovations;
create policy "Acesso leitura seguro" on public.data_innovations for select
using (auth.role() = 'authenticated' and public.is_approved());

drop policy if exists "Acesso escrita admin" on public.data_innovations;
create policy "Acesso escrita admin" on public.data_innovations for all
using (auth.role() = 'authenticated' and public.is_admin())
with check (auth.role() = 'authenticated' and public.is_admin());

-- Metadata
drop policy if exists "Enable read access for all users" on public.data_metadata;
drop policy if exists "Acesso leitura aprovados" on public.data_metadata;
create policy "Acesso leitura seguro" on public.data_metadata for select
using (auth.role() = 'authenticated' and public.is_approved());

drop policy if exists "Acesso escrita admin" on public.data_metadata;
create policy "Acesso escrita admin" on public.data_metadata for all
using (auth.role() = 'authenticated' and public.is_admin())
with check (auth.role() = 'authenticated' and public.is_admin());

-- Goals Distribution
-- Permite leitura para aprovados
drop policy if exists "Enable read access for all users" on public.goals_distribution;
drop policy if exists "Acesso leitura aprovados" on public.goals_distribution;
create policy "Acesso leitura seguro" on public.goals_distribution for select
using (auth.role() = 'authenticated' and public.is_approved());

-- Permite escrita (insert/update) APENAS para ADMINS (role='adm')
drop policy if exists "Enable insert/update for goals" on public.goals_distribution;
drop policy if exists "Acesso escrita aprovados" on public.goals_distribution;
create policy "Acesso escrita admin" on public.goals_distribution for all
using (auth.role() = 'authenticated' and public.is_admin())
with check (auth.role() = 'authenticated' and public.is_admin());


-- Profiles Policies (Mantidas padrão)
drop policy if exists "Users can view own profile" on public.profiles;
create policy "Users can view own profile" on public.profiles for select
using (auth.uid() = id);

drop policy if exists "Users can update own profile" on public.profiles;
create policy "Users can update own profile" on public.profiles for update
using (auth.uid() = id);


-- Trigger para criar profile ao cadastrar (Mantido)
create or replace function public.handle_new_user () returns trigger language plpgsql security definer
set search_path = public as $$
begin
  insert into public.profiles (id, email, status)
  values (new.id, new.email, 'pendente');
  return new;
end;
$$;

drop trigger if exists on_auth_user_created on auth.users;
create trigger on_auth_user_created
after insert on auth.users for each row
execute procedure public.handle_new_user ();
