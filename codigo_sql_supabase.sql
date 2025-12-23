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

-- Ensure columns exist even if table was created previously
alter table public.data_detailed
add column if not exists observacaofor text;

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

alter table public.data_history
add column if not exists observacaofor text;

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

-- 4. Tabela de Pedidos Agregados (Otimização para lista de pedidos)
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
  codfors_list text[]
);

-- 5. Tabela de Detalhes de Produtos
create table if not exists public.data_product_details (
  code text primary key,
  descricao text,
  fornecedor text,
  codfor text,
  dtcadastro timestamp with time zone,
  pasta text
);

-- Ensure pasta column exists
alter table public.data_product_details
add column if not exists pasta text;

-- 6. Tabela de Produtos Ativos (Apenas códigos)
create table if not exists public.data_active_products (code text primary key);

-- 7. Tabela de Estoque
create table if not exists public.data_stock (
  id uuid default uuid_generate_v4 () primary key,
  product_code text,
  filial text,
  stock_qty numeric
);

-- 8. Tabela de Inovações (Metas/Status)
create table if not exists public.data_innovations (
  id uuid default uuid_generate_v4 () primary key,
  codigo text, -- Código do Produto
  produto text, -- Nome/Descrição
  inovacoes text -- Categoria ou Status
);

-- 9. Tabela de Metadados (Data de atualização, dias úteis, etc)
create table if not exists public.data_metadata (key text primary key, value text);

-- 10. Tabela para Salvar Metas (Novo Recurso)
create table if not exists public.goals_distribution (
  id uuid default uuid_generate_v4 () primary key,
  month_key text not null, -- Ex: '2023-10'
  supplier text not null, -- Ex: 'PEPSICO_ALL', '707'
  brand text default 'GENERAL', -- Ex: 'TODDYNHO' (default 'GENERAL')
  goals_data jsonb not null, -- Estrutura com as metas por cliente/vendedor
  updated_at timestamp with time zone default now(),
  updated_by text -- Opcional: ID do usuário que atualizou
);

-- Criação de Índices para Performance
-- Indexes commented out based on Supabase "Unused Index" linter warnings.
-- Uncomment them if your application queries start filtering by these fields.
drop index if exists public.idx_detailed_codcli;

-- create index if not exists idx_detailed_codcli on public.data_detailed(codcli);
create index if not exists idx_detailed_codusur on public.data_detailed (codusur);

drop index if exists public.idx_detailed_produto;

-- create index if not exists idx_detailed_produto on public.data_detailed(produto);
drop index if exists public.idx_history_codcli;

-- create index if not exists idx_history_codcli on public.data_history(codcli);
drop index if exists public.idx_history_codusur;

-- create index if not exists idx_history_codusur on public.data_history(codusur);
drop index if exists public.idx_clients_codigo;

-- create index if not exists idx_clients_codigo on public.data_clients(codigo_cliente);
drop index if exists public.idx_clients_rca1;

-- create index if not exists idx_clients_rca1 on public.data_clients(rca1);
drop index if exists public.idx_stock_product;

-- create index if not exists idx_stock_product on public.data_stock(product_code);
drop index if exists public.idx_goals_distribution_updated_by;

create unique index if not exists idx_goals_unique on public.goals_distribution (month_key, supplier, brand);

-- RLS (Row Level Security)
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

-- Políticas de Leitura (Permitir leitura pública ou para autenticados)
-- Ajuste conforme necessidade: 'anon' para público, 'authenticated' para logados.
-- Usando DROP IF EXISTS antes para evitar erros ao re-executar o script.
drop policy if exists "Authenticated can read data_detailed" on public.data_detailed;

drop policy if exists "Enable read access for all users" on public.data_detailed;

create policy "Enable read access for all users" on public.data_detailed for
select
  using (true);

drop policy if exists "Authenticated can read data_history" on public.data_history;

drop policy if exists "Enable read access for all users" on public.data_history;

create policy "Enable read access for all users" on public.data_history for
select
  using (true);

drop policy if exists "Authenticated can read data_clients" on public.data_clients;

drop policy if exists "Enable read access for all users" on public.data_clients;

create policy "Enable read access for all users" on public.data_clients for
select
  using (true);

drop policy if exists "Authenticated can read data_orders" on public.data_orders;

drop policy if exists "Enable read access for all users" on public.data_orders;

create policy "Enable read access for all users" on public.data_orders for
select
  using (true);

drop policy if exists "Authenticated can read data_product_details" on public.data_product_details;

drop policy if exists "Enable read access for all users" on public.data_product_details;

create policy "Enable read access for all users" on public.data_product_details for
select
  using (true);

drop policy if exists "Authenticated can read data_active_products" on public.data_active_products;

drop policy if exists "Enable read access for all users" on public.data_active_products;

create policy "Enable read access for all users" on public.data_active_products for
select
  using (true);

drop policy if exists "Authenticated can read data_stock" on public.data_stock;

drop policy if exists "Enable read access for all users" on public.data_stock;

create policy "Enable read access for all users" on public.data_stock for
select
  using (true);

drop policy if exists "Authenticated can read data_innovations" on public.data_innovations;

drop policy if exists "Enable read access for all users" on public.data_innovations;

create policy "Enable read access for all users" on public.data_innovations for
select
  using (true);

drop policy if exists "Authenticated can read data_metadata" on public.data_metadata;

drop policy if exists "Enable read access for all users" on public.data_metadata;

create policy "Enable read access for all users" on public.data_metadata for
select
  using (true);

-- Removida a política redundante de leitura para goals_distribution para evitar avisos de Múltiplas Políticas Permissivas.
-- A política "Enable insert/update for goals" abaixo cobre o acesso se estiver configurada corretamente.
drop policy if exists "Enable read access for all users" on public.goals_distribution;

drop policy if exists "Goals: managers or owner can modify" on public.goals_distribution;

-- Políticas de Escrita (Geralmente restritas a service_role ou admins)
-- Como o upload é feito via chave service_role no backend ou cliente com chave especifica, 
-- o service_role bypassa o RLS.
-- Se for necessário permitir insert via anon (não recomendado sem proteção), descomente:
-- create policy "Enable insert for all users" on public.data_detailed for insert with check (true);
-- ... (repetir para outras tabelas se necessário)
-- Para a tabela de metas, permitir insert/update para autenticados (ou todos se controlado via app)
drop policy if exists "Enable insert/update for goals" on public.goals_distribution;

create policy "Enable insert/update for goals" on public.goals_distribution for all using (true)
with
  check (true);

-- 11. Tabela de Perfis de Usuário (Gatekeeper)
create table if not exists public.profiles (
  id uuid references auth.users on delete cascade not null primary key,
  email text,
  status text default 'pendente', -- pendente, aprovado, bloqueado
  role text default 'user',
  created_at timestamp with time zone default now(),
  updated_at timestamp with time zone default now()
);

-- RLS para Profiles
alter table public.profiles enable row level security;

-- Usuários podem ver seu próprio perfil
drop policy if exists "Users can view own profile" on public.profiles;

create policy "Users can view own profile" on public.profiles for
select
  using (
    (
      select
        auth.uid ()
    ) = id
  );

-- Usuários podem atualizar seu próprio perfil
drop policy if exists "Users can update own profile" on public.profiles;

create policy "Users can update own profile" on public.profiles
for update
  using (
    (
      select
        auth.uid ()
    ) = id
  );

-- Função para criar perfil automaticamente no cadastro
create or replace function public.handle_new_user () returns trigger language plpgsql security definer
set
  search_path = public as $$
begin
  insert into public.profiles (id, email, status)
  values (new.id, new.email, 'pendente');
  return new;
end;
$$;

-- Trigger para chamar a função acima
drop trigger if exists on_auth_user_created on auth.users;

create trigger on_auth_user_created
after insert on auth.users for each row
execute procedure public.handle_new_user ();
