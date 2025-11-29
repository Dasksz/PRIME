/*
  SCRIPT SQL v4.4 (Tudo Colunar)
  Instruções:
  1. Cole e execute este script UMA VEZ no SQL Editor do Supabase.
  2. Isto irá APAGAR todas as tabelas antigas (com erros de UUID)
     e recriá-las com a estrutura correta (usando TEXT).
  3. Este script está alinhado com o uploader V5.1 (V5-corrigido.html).
*/

-- ETAPA 1: Apagar todas as tabelas antigas (Garantia)
DROP TABLE IF EXISTS data_detailed;
DROP TABLE IF EXISTS data_history;
DROP TABLE IF EXISTS data_clients;
DROP TABLE IF EXISTS data_orders;
DROP TABLE IF EXISTS data_products; -- Tabela JSONB antiga (se existir)
DROP TABLE IF EXISTS data_stock;
DROP TABLE IF EXISTS data_innovations;
DROP TABLE IF EXISTS data_metadata;
DROP TABLE IF EXISTS data_product_details;
DROP TABLE IF EXISTS data_active_products;


-- ETAPA 2: Criar as 9 tabelas como estruturas colunares

-- Tabela 1: Vendas Detalhadas (detailed)
CREATE TABLE data_detailed (
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
alter table data_detailed enable row level security;
create policy "Permitir leitura pública detailed" on data_detailed
for select using (true);

-- Tabela 2: Histórico de Vendas (history)
CREATE TABLE data_history (
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
alter table data_history enable row level security;
create policy "Permitir leitura pública history" on data_history
for select using (true);

-- Tabela 3: Clientes (clients)
CREATE TABLE data_clients (
    codigo_cliente text primary key, -- CORRIGIDO: Chave é TEXT (corrige erro UUID)
    rcas text[],
    rca1 text,
    rca2 text,
    cidade text,
    nomeCliente text,
    bairro text,
    razaoSocial text,
    fantasia text,
    cnpj_cpf text,
    endereco text,
    numero text,
    cep text,
    telefone text,
    email text,
    ramo text,
    ultimaCompra text,
    "dataCadastro" text,
    bloqueio text,
    "inscricaoEstadual" text
);
alter table data_clients enable row level security;
create policy "Permitir leitura pública clients" on data_clients
for select using (true);


-- Tabela 4: Pedidos Agregados (byOrder)
CREATE TABLE data_orders (
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
alter table data_orders enable row level security;
create policy "Permitir leitura pública orders" on data_orders
for select using (true);


-- Tabela 5: Detalhes dos Produtos (productDetails)
CREATE TABLE data_product_details (
    code text primary key, -- CORRIGIDO: Chave é TEXT (corrige erro UUID)
    descricao text,
    fornecedor text,
    codfor text,
    dtCadastro text
);
alter table data_product_details enable row level security;
create policy "Permitir leitura pública product_details" on data_product_details
for select using (true);


-- Tabela 6: Códigos de Produtos Ativos (activeProductCodes)
CREATE TABLE data_active_products (
      code text primary key -- CORRIGIDO: Chave é TEXT (corrige erro UUID)
);
alter table data_active_products enable row level security;
create policy "Permitir leitura pública active_products" on data_active_products
for select using (true);


-- Tabela 7: Estoque (stockMap05, stockMap08)
CREATE TABLE data_stock (
    id bigserial primary key,
    product_code text, -- CORRIGIDO: Coluna é TEXT (corrige erro UUID)
    filial text,
    stock_qty numeric
);
alter table data_stock enable row level security;
create policy "Permitit leitura pública stock" on data_stock
for select using (true);
CREATE INDEX idx_stock_product_filial ON data_stock (product_code, filial);


-- Tabela 8: Inovações (innovationsMonth)
CREATE TABLE data_innovations (
    id bigserial primary key,
    inovacoes text,
    codigo text,
    produto text
);
alter table data_innovations enable row level security;
create policy "Permitir leitura pública innovations" on data_innovations
for select using (true);


-- Tabela 9: Metadados (passedWorkingDaysCurrentMonth)
CREATE TABLE data_metadata (
    key text primary key,
    value text
);
alter table data_metadata enable row level security;
create policy "Permitir leitura pública metadata" on data_metadata
for select using (true);