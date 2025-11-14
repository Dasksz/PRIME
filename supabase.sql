-- =================================================================
-- SCRIPT SQL UNIFICADO V2.4 - PRIME (CORREÇÃO GERAL)
-- OBJETIVO: Corrigir erros de tipo de dados nas funções RPC e
--           adicionar índice de performance para a tabela de perfis.
-- =================================================================
-- ETAPA 1: POLÍTICAS DE SEGURANÇA (RLS) - (Inalterado)
-- 1.1: Função Auxiliar de Segurança
create or replace function public.is_caller_approved () RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER
set
  search_path = public as $$
  SELECT EXISTS (
    SELECT 1 FROM profiles
    WHERE id = (SELECT auth.uid()) AND status = 'aprovado'
  );
$$;

-- 1.2: Políticas RLS para Tabelas de Dados
do $$
DECLARE
    tbl_name TEXT;
BEGIN
    FOR tbl_name IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name LIKE 'data_%'
    LOOP
        EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY;', tbl_name);

        EXECUTE format('
            DROP POLICY IF EXISTS "Permitir acesso total para service_role ou aprovados" ON public.%I;
            CREATE POLICY "Permitir acesso total para service_role ou aprovados" ON public.%I
            FOR ALL
            USING (
              ((SELECT auth.role()) = ''service_role'') OR (public.is_caller_approved())
            )
            WITH CHECK (
              ((SELECT auth.role()) = ''service_role'') OR (public.is_caller_approved())
            );
        ', tbl_name, tbl_name, tbl_name);
    END LOOP;
END $$;

-- 1.3: Políticas RLS para Perfis (Profiles)
alter table public.profiles ENABLE row LEVEL SECURITY;
drop policy IF exists "Perfis: Usuários autenticados podem ver o próprio perfil" on public.profiles;
drop policy IF exists "Perfis: Usuários autenticados podem criar o próprio perfil" on public.profiles;
drop policy IF exists "Perfis: Usuários autenticados podem atualizar o próprio perfil" on public.profiles;
create policy "Perfis: Usuários autenticados podem ver o próprio perfil" on public.profiles for
select
  using ( (
    select
      auth.uid ()
  ) = id
  );
create policy "Perfis: Usuários autenticados podem criar o próprio perfil" on public.profiles for INSERT
with
  check ( (
    select
      auth.uid ()
  ) = id
  );
create policy "Perfis: Usuários autenticados podem atualizar o próprio perfil" on public.profiles
for update
  using ( (
    select
      auth.uid ()
  ) = id
  )
with
  check ( (
    select
      auth.uid ()
  ) = id
  );

-- =================================================================
-- ETAPA 2: ÍNDICES (INDEXES) - (Índice de Perfis Adicionado)
-- =================================================================
-- 2.1: Índices para data_detailed
create index IF not exists idx_detailed_dtped on public.data_detailed (dtped);
create index IF not exists idx_detailed_superv on public.data_detailed (superv);
create index IF not exists idx_detailed_codusur on public.data_detailed (codusur);
create index IF not exists idx_detailed_nome on public.data_detailed (nome);
create index IF not exists idx_detailed_codcli on public.data_detailed (codcli);
create index IF not exists idx_detailed_cidade on public.data_detailed (cidade);
create index IF not exists idx_detailed_observacaofor on public.data_detailed (observacaofor);
create index IF not exists idx_detailed_codfor on public.data_detailed (codfor);
create index IF not exists idx_detailed_produto on public.data_detailed (produto);
create index IF not exists idx_detailed_posicao on public.data_detailed (posicao);
create index IF not exists idx_detailed_tipovenda on public.data_detailed (tipovenda);
create index IF not exists idx_detailed_filial on public.data_detailed (filial);

-- 2.2: Índices para data_history
create index IF not exists idx_history_dtped on public.data_history (dtped);
create index IF not exists idx_history_superv on public.data_history (superv);
create index IF not exists idx_history_codusur on public.data_history (codusur);
create index IF not exists idx_history_nome on public.data_history (nome);
create index IF not exists idx_history_codcli on public.data_history (codcli);
create index IF not exists idx_history_cidade on public.data_history (cidade);
create index IF not exists idx_history_observacaofor on public.data_history (observacaofor);
create index IF not exists idx_history_codfor on public.data_history (codfor);
create index IF not exists idx_history_produto on public.data_history (produto);
create index IF not exists idx_history_filial on public.data_history (filial);

-- 2.3: Índices para data_clients
create index IF not exists idx_clients_ramo on public.data_clients (ramo);
create index IF not exists idx_clients_rca1 on public.data_clients (rca1);
create index IF not exists idx_clients_cidade on public.data_clients (cidade);
create index IF not exists idx_clients_codigo_cliente on public.data_clients (codigo_cliente);

-- 2.4: Índices para data_orders
create index IF not exists idx_orders_dtped on public.data_orders (dtped);
create index IF not exists idx_orders_superv on public.data_orders (superv);
create index IF not exists idx_orders_nome on public.data_orders (nome);
create index IF not exists idx_orders_codcli on public.data_orders (codcli);
create index IF not exists idx_orders_posicao on public.data_orders (posicao);
create index IF not exists idx_orders_codfors_list on public.data_orders using GIN (codfors_list);
create index IF not exists idx_orders_fornecedores_list on public.data_orders using GIN (fornecedores_list);

-- 2.5: Índices para data_product_details
create index IF not exists idx_product_details_code on public.data_product_details (code);
create index IF not exists idx_product_details_codfor on public.data_product_details (codfor);

-- 2.6: Índice para data_stock
create index IF not exists idx_stock_product_code on public.data_stock (product_code);
create index IF not exists idx_stock_filial on public.data_stock (filial);

-- 2.7: Índice para profiles (NOVO)
create index IF not exists profiles_id_idx on public.profiles (id);


-- =================================================================
-- ETAPA 3: FUNÇÕES DE CÁLCULO (RPC) - (Funções Corrigidas)
-- =================================================================
-- 3.0: Função Auxiliar de Filtro de Cliente (BASE) - (Inalterado)
create or replace function get_filtered_client_base (
  p_supervisor TEXT default null,
  p_vendedor_nomes text[] default null,
  p_rede_group TEXT default null,
  p_redes text[] default null,
  p_cidade TEXT default null,
  p_codcli TEXT default null,
  p_filial TEXT default null
) RETURNS table (codigo_cliente TEXT) LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    RETURN QUERY
    WITH ClientLastBranch AS (
        SELECT DISTINCT ON (codcli) codcli, filial
        FROM (
            SELECT codcli, filial, dtped FROM public.data_detailed
            UNION ALL
            SELECT codcli, filial, dtped FROM public.data_history
        ) AS all_sales
        ORDER BY codcli, dtped DESC
    )
    SELECT c.codigo_cliente
    FROM public.data_clients AS c
    LEFT JOIN ClientLastBranch AS clb ON c.codigo_cliente = clb.codcli
    WHERE
        (p_rede_group IS NULL OR
         (p_rede_group = 'sem_rede' AND (c.ramo IS NULL OR c.ramo = 'N/A')) OR
         (p_rede_group = 'com_rede' AND (c.ramo IS NOT NULL AND c.ramo != 'N/A') AND
            (p_redes IS NULL OR c.ramo = ANY(p_redes)))
        )
    AND (p_supervisor IS NULL OR c.rca1 IN (
            SELECT DISTINCT codusur FROM data_detailed WHERE superv = p_supervisor
            UNION
            SELECT DISTINCT codusur FROM data_history WHERE superv = p_supervisor
        ))
    AND (p_vendedor_nomes IS NULL OR c.rca1 IN (
            SELECT DISTINCT codusur FROM data_detailed WHERE nome = ANY(p_vendedor_nomes)
            UNION
            SELECT DISTINCT codusur FROM data_history WHERE nome = ANY(p_vendedor_nomes)
        ))
    AND (p_cidade IS NULL OR c.cidade = p_cidade)
    AND (p_codcli IS NULL OR c.codigo_cliente = p_codcli)
    AND (p_filial = 'ambas' OR p_filial IS NULL OR clb.filial = p_filial);
END;
$$;

-- 3.1: KPIs Principais (CORRIGIDO)
create or replace function get_main_kpis (
  p_pasta TEXT default null,
  p_supervisor TEXT default null,
  p_vendedor_nomes text[] default null,
  p_codcli TEXT default null,
  p_posicao TEXT default null,
  p_codfor TEXT default null,
  p_tipos_venda text[] default null,
  p_rede_group TEXT default null,
  p_redes text[] default null,
  p_cidade TEXT default null,
  p_filial TEXT default 'ambas'
) RETURNS table (
  total_faturamento NUMERIC,
  total_peso NUMERIC,
  total_skus BIGINT,
  total_pdvs_positivados BIGINT,
  base_clientes_filtro BIGINT
) LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
DECLARE
    v_base_clientes_count BIGINT;
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    SELECT COUNT(*)
    INTO v_base_clientes_count
    FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial);

    RETURN QUERY
    WITH VendasFiltradas AS (
        SELECT v.vlvenda, v.vlbonific, v.totpesoliq, v.codcli, v.produto
        FROM public.data_detailed AS v
        WHERE
            (p_pasta IS NULL OR v.observacaofor = p_pasta)
        AND (p_supervisor IS NULL OR v.superv = p_supervisor)
        AND (p_vendedor_nomes IS NULL OR v.nome = ANY(p_vendedor_nomes))
        AND (p_codcli IS NULL OR v.codcli = p_codcli)
        AND (p_posicao IS NULL OR v.posicao = p_posicao)
        AND (p_codfor IS NULL OR v.codfor = p_codfor)
        AND (p_tipos_venda IS NULL OR v.tipovenda = ANY(p_tipos_venda))
        AND (p_cidade IS NULL OR v.cidade = p_cidade)
        AND (p_filial = 'ambas' OR p_filial IS NULL OR v.filial = p_filial)
        AND (p_rede_group IS NULL AND p_supervisor IS NULL AND p_vendedor_nomes IS NULL AND p_cidade IS NULL AND p_codcli IS NULL) OR v.codcli IN (
            SELECT codigo_cliente FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial)
        )
    )
    SELECT
        COALESCE(SUM(v.vlvenda), 0::numeric) AS total_faturamento,
        COALESCE(SUM(v.totpesoliq), 0::numeric) AS total_peso,
        COUNT(DISTINCT v.produto) AS total_skus,
        COUNT(DISTINCT v.codcli) AS total_pdvs_positivados,
        v_base_clientes_count AS base_clientes_filtro
    FROM VendasFiltradas AS v
    WHERE v.vlvenda > 0 OR v.vlbonific > 0;
END;
$$;

-- 3.2: Gráficos de Barras (Inalterado)
create or replace function get_sales_by_group (
  p_group_by TEXT,
  p_pasta TEXT default null,
  p_supervisor TEXT default null,
  p_vendedor_nomes text[] default null,
  p_codcli TEXT default null,
  p_posicao TEXT default null,
  p_codfor TEXT default null,
  p_tipos_venda text[] default null,
  p_rede_group TEXT default null,
  p_redes text[] default null,
  p_cidade TEXT default null,
  p_filial TEXT default 'ambas'
) RETURNS table (group_name TEXT, total_faturamento NUMERIC) LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    RETURN QUERY
    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial)
    ),
    VendasFiltradas AS (
        SELECT v.vlvenda, v.superv, v.nome, v.observacaofor
        FROM public.data_detailed AS v
        WHERE
            (p_pasta IS NULL OR v.observacaofor = p_pasta)
        AND (p_supervisor IS NULL OR v.superv = p_supervisor)
        AND (p_vendedor_nomes IS NULL OR v.nome = ANY(p_vendedor_nomes))
        AND (p_codcli IS NULL OR v.codcli = p_codcli)
        AND (p_posicao IS NULL OR v.posicao = p_posicao)
        AND (p_codfor IS NULL OR v.codfor = p_codfor)
        AND (p_tipos_venda IS NULL OR v.tipovenda = ANY(p_tipos_venda))
        AND (p_cidade IS NULL OR v.cidade = p_cidade)
        AND (p_filial = 'ambas' OR p_filial IS NULL OR v.filial = p_filial)
        AND (p_rede_group IS NULL AND p_supervisor IS NULL AND p_vendedor_nomes IS NULL AND p_cidade IS NULL AND p_codcli IS NULL) OR v.codcli IN (SELECT codigo_cliente FROM ClientBase)
    )
    SELECT
        CASE
            WHEN p_group_by = 'supervisor' THEN v.superv
            WHEN p_group_by = 'vendedor' THEN v.nome
            WHEN p_group_by = 'categoria' THEN v.observacaofor
            ELSE 'N/A'
        END AS group_name,
        SUM(v.vlvenda) AS total_faturamento
    FROM VendasFiltradas AS v
    GROUP BY group_name
    ORDER BY total_faturamento DESC
    LIMIT 20;
END;
$$;

-- 3.3: Top Produtos (CORRIGIDO)
create or replace function get_top_products (
  p_metric TEXT,
  p_pasta TEXT default null,
  p_supervisor TEXT default null,
  p_vendedor_nomes text[] default null,
  p_codcli TEXT default null,
  p_posicao TEXT default null,
  p_codfor TEXT default null,
  p_tipos_venda text[] default null,
  p_rede_group TEXT default null,
  p_redes text[] default null,
  p_cidade TEXT default null,
  p_filial TEXT default 'ambas'
) RETURNS table (
  codigo_produto TEXT,
  descricao_produto TEXT,
  valor_metrica NUMERIC
) LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    RETURN QUERY
    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial)
    )
    SELECT
        v.produto::text AS codigo_produto,
        pd.descricao AS descricao_produto,
        COALESCE(SUM(
            CASE
                WHEN p_metric = 'faturamento' THEN v.vlvenda
                WHEN p_metric = 'peso' THEN v.totpesoliq
                ELSE 0::numeric
            END
        ), 0::numeric) AS valor_metrica
    FROM
        public.data_detailed AS v
    JOIN
        public.data_product_details AS pd ON v.produto = pd.code
    WHERE
        (p_pasta IS NULL OR v.observacaofor = p_pasta)
    AND (p_supervisor IS NULL OR v.superv = p_supervisor)
    AND (p_vendedor_nomes IS NULL OR v.nome = ANY(p_vendedor_nomes))
    AND (p_codcli IS NULL OR v.codcli = p_codcli)
    AND (p_posicao IS NULL OR v.posicao = p_posicao)
    AND (p_codfor IS NULL OR v.codfor = p_codfor)
    AND (p_tipos_venda IS NULL OR v.tipovenda = ANY(p_tipos_venda))
    AND (p_cidade IS NULL OR v.cidade = p_cidade)
    AND (p_filial = 'ambas' OR p_filial IS NULL OR v.filial = p_filial)
    AND (p_rede_group IS NULL AND p_supervisor IS NULL AND p_vendedor_nomes IS NULL AND p_cidade IS NULL AND p_codcli IS NULL) OR v.codcli IN (SELECT codigo_cliente FROM ClientBase)
    GROUP BY
        v.produto, pd.descricao
    ORDER BY
        valor_metrica DESC
    LIMIT 10;
END;
$$;

-- 3.4: Tabela de Pedidos Paginada (Inalterado)
create or replace function get_paginated_orders (
  p_page_number INT,
  p_page_size INT,
  p_pasta TEXT default null,
  p_supervisor TEXT default null,
  p_vendedor_nomes text[] default null,
  p_codcli TEXT default null,
  p_posicao TEXT default null,
  p_codfor TEXT default null,
  p_tipos_venda text[] default null,
  p_rede_group TEXT default null,
  p_redes text[] default null,
  p_cidade TEXT default null,
  p_filial TEXT default 'ambas'
) RETURNS SETOF data_orders LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    RETURN QUERY
    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial)
    )
    SELECT *
    FROM public.data_orders AS o
    WHERE
        (p_pasta IS NULL OR (o.fornecedores_list::text[] @> ARRAY[p_pasta]))
    AND (p_supervisor IS NULL OR o.superv = p_supervisor)
    AND (p_vendedor_nomes IS NULL OR o.nome = ANY(p_vendedor_nomes))
    AND (p_codcli IS NULL OR o.codcli = p_codcli)
    AND (p_posicao IS NULL OR o.posicao = p_posicao)
    AND (p_codfor IS NULL OR (o.codfors_list::text[] @> ARRAY[p_codfor]))
    AND (p_tipos_venda IS NULL OR o.tipovenda = ANY(p_tipos_venda))
    AND (p_cidade IS NULL OR o.cidade = p_cidade)
    AND (p_filial = 'ambas' OR p_filial IS NULL OR o.filial = p_filial)
    AND (p_rede_group IS NULL AND p_supervisor IS NULL AND p_vendedor_nomes IS NULL AND p_cidade IS NULL AND p_codcli IS NULL) OR o.codcli IN (SELECT codigo_cliente FROM ClientBase)
    ORDER BY
        o.dtped DESC, o.pedido DESC
    LIMIT p_page_size
    OFFSET (p_page_number - 1) * p_page_size;
END;
$$;

-- 3.5: Contagem de Pedidos (Inalterado)
create or replace function get_orders_count (
  p_pasta TEXT default null,
  p_supervisor TEXT default null,
  p_vendedor_nomes text[] default null,
  p_codcli TEXT default null,
  p_posicao TEXT default null,
  p_codfor TEXT default null,
  p_tipos_venda text[] default null,
  p_rede_group TEXT default null,
  p_redes text[] default null,
  p_cidade TEXT default null,
  p_filial TEXT default 'ambas'
) RETURNS BIGINT LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
DECLARE
    total_count BIGINT;
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial)
    )
    SELECT COUNT(*)
    INTO total_count
    FROM public.data_orders AS o
    WHERE
        (p_pasta IS NULL OR (o.fornecedores_list::text[] @> ARRAY[p_pasta]))
    AND (p_supervisor IS NULL OR o.superv = p_supervisor)
    AND (p_vendedor_nomes IS NULL OR o.nome = ANY(p_vendedor_nomes))
    AND (p_codcli IS NULL OR o.codcli = p_codcli)
    AND (p_posicao IS NULL OR o.posicao = p_posicao)
    AND (p_codfor IS NULL OR (o.codfors_list::text[] @> ARRAY[p_codfor]))
    AND (p_tipos_venda IS NULL OR o.tipovenda = ANY(p_tipos_venda))
    AND (p_cidade IS NULL OR o.cidade = p_cidade)
    AND (p_filial = 'ambas' OR p_filial IS NULL OR o.filial = p_filial)
    AND (p_rede_group IS NULL AND p_supervisor IS NULL AND p_vendedor_nomes IS NULL AND p_cidade IS NULL AND p_codcli IS NULL) OR o.codcli IN (SELECT codigo_cliente FROM ClientBase);

    RETURN total_count;
END;
$$;

-- 3.6: Funções para o Ecrã 'city-view' (Inalterado)
create or replace function get_city_analysis (
  p_supervisor TEXT default null,
  p_vendedor_nomes text[] default null,
  p_rede_group TEXT default null,
  p_redes text[] default null,
  p_cidade TEXT default null,
  p_codcli TEXT default null
) RETURNS table (
  tipo_analise TEXT,
  group_name TEXT,
  total_faturamento NUMERIC,
  codigo_cliente TEXT,
  fantasia TEXT,
  cidade TEXT,
  bairro TEXT,
  ultimacompra DATE,
  rca1 TEXT,
  status_cliente TEXT
) LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
DECLARE
    v_current_month DATE := date_trunc('month', NOW());
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    RETURN QUERY
    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, 'ambas')
    ),
    SalesThisMonth AS (
        SELECT
            codcli,
            cidade,
            bairro,
            SUM(vlvenda) AS total_faturado
        FROM public.data_detailed
        WHERE dtped >= v_current_month AND vlvenda > 0
          AND codcli IN (SELECT cb.codigo_cliente FROM ClientBase cb)
        GROUP BY codcli, cidade, bairro
    )
    -- 1. Dados para os Gráficos
    (SELECT
        'chart' AS tipo_analise,
        CASE
            WHEN p_cidade IS NOT NULL THEN s.bairro
            ELSE s.cidade
        END AS group_name,
        SUM(s.total_faturado) AS total_faturamento,
        NULL::text, NULL::text, NULL::text, NULL::text, NULL::date, NULL::text, NULL::text
    FROM SalesThisMonth AS s
    GROUP BY group_name
    ORDER BY total_faturamento DESC
    LIMIT 10)

    UNION ALL

    -- 2. Dados para as Tabelas de Clientes
    SELECT
        'client_list' AS tipo_analise,
        NULL::text,
        COALESCE(s.total_faturado, 0) AS total_faturamento,
        c.codigo_cliente,
        COALESCE(c.fantasia, c.razaosocial),
        c.cidade,
        c.bairro,
        c.ultimacompra,
        c.rca1,
        CASE
            WHEN s.codcli IS NOT NULL THEN 'ativo'
            WHEN c.datacadastro >= v_current_month THEN 'novo'
            ELSE 'inativo'
        END AS status_cliente
    FROM ClientBase AS c
    LEFT JOIN SalesThisMonth AS s ON c.codigo_cliente = s.codcli
    WHERE
      c.bloqueio != 'S' AND c.rca1 != '53';
END;
$$;

-- 3.7: Funções para o Ecrã 'weekly-view' (Inalterado)
create or replace function get_weekly_sales_and_rankings (
  p_pasta TEXT default null,
  p_supervisores text[] default null
) RETURNS table (
  tipo_dado TEXT,
  group_name TEXT,
  week_num INT,
  total_valor NUMERIC
) LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
DECLARE
    v_current_month DATE := date_trunc('month', NOW());
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    RETURN QUERY
    WITH VendasFiltradas AS (
        SELECT
            vlvenda, superv, nome, codcli, produto, codfor,
            EXTRACT(ISODOW FROM dtped) AS dia_semana,
            (date_part('day', dtped - date_trunc('month', dtped)) / 7 + 1)::int AS semana_mes
        FROM public.data_detailed
        WHERE dtped >= v_current_month
          AND (p_pasta IS NULL OR observacaofor = p_pasta)
          AND (p_supervisores IS NULL OR superv = ANY(p_supervisores))
          AND superv != 'BALCAO'
    )
    -- 1. Dados para o Gráfico de Vendas Semanais
    SELECT
        'venda_semanal' AS tipo_dado,
        dia_semana::text AS group_name,
        semana_mes AS week_num,
        SUM(vlvenda) AS total_valor
    FROM VendasFiltradas
    WHERE dia_semana BETWEEN 1 AND 5
    GROUP BY semana_mes, dia_semana

    UNION ALL

    -- 2. Ranking de Positivação
    (SELECT
        'rank_positivacao' AS tipo_dado,
        nome AS group_name,
        NULL::int,
        COUNT(DISTINCT codcli) AS total_valor
    FROM VendasFiltradas
    GROUP BY nome
    ORDER BY total_valor DESC
    LIMIT 10)

    UNION ALL

    -- 3. Ranking Top Sellers
    (SELECT
        'rank_topsellers' AS tipo_dado,
        nome AS group_name,
        NULL::int,
        SUM(vlvenda) AS total_valor
    FROM VendasFiltradas
    GROUP BY nome
    ORDER BY total_valor DESC
    LIMIT 10)

    UNION ALL

    -- 4. Ranking de Mix
    (SELECT
        'rank_mix' AS tipo_dado,
        nome,
        NULL::int,
        AVG(mix_count) AS total_valor
    FROM (
        SELECT
            nome,
            codcli,
            COUNT(DISTINCT produto) AS mix_count
        FROM (
            SELECT * FROM VendasFiltradas
            WHERE superv = 'OSVALDO NUNES O' OR codfor IN ('707', '708')
        ) AS VendasMix
        GROUP BY nome, codcli
    ) AS MixPorCliente
    GROUP BY nome
    ORDER BY total_valor DESC
    LIMIT 10);
END;
$$;

-- 3.8: Funções para o Ecrã 'comparison-view' (CORRIGIDO)
create or replace function get_comparison_data (
  p_pasta TEXT default null,
  p_supervisor TEXT default null,
  p_vendedor_nomes text[] default null,
  p_codcli TEXT default null,
  p_fornecedores text[] default null,
  p_produtos text[] default null,
  p_rede_group TEXT default null,
  p_redes text[] default null,
  p_cidade TEXT default null,
  p_filial TEXT default 'ambas'
) RETURNS table (
  origem TEXT,
  superv TEXT,
  dtped DATE,
  vlvenda NUMERIC,
  totpesoliq NUMERIC,
  codcli TEXT,
  produto TEXT,
  descricao TEXT,
  codfor TEXT
) LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    RETURN QUERY
    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial)
    )
    -- 1. Vendas Atuais (Mês Atual)
    SELECT
        'current' AS origem, v.superv, v.dtped::date, v.vlvenda,
        v.totpesoliq, v.codcli, v.produto, pd.descricao, v.codfor
    FROM public.data_detailed AS v
    JOIN public.data_product_details AS pd ON v.produto = pd.code
    WHERE
        (p_pasta IS NULL OR v.observacaofor = p_pasta)
    AND (p_supervisor IS NULL OR v.superv = p_supervisor)
    AND (p_vendedor_nomes IS NULL OR v.nome = ANY(p_vendedor_nomes))
    AND (p_codcli IS NULL OR v.codcli = p_codcli)
    AND (p_fornecedores IS NULL OR v.codfor = ANY(p_fornecedores))
    AND (p_produtos IS NULL OR v.produto = ANY(p_produtos))
    AND (p_cidade IS NULL OR v.cidade = p_cidade)
    AND (p_filial = 'ambas' OR p_filial IS NULL OR v.filial = p_filial)
    AND v.codcli IN (SELECT codigo_cliente FROM ClientBase)

    UNION ALL

    -- 2. Vendas Históricas
    SELECT
        'history' AS origem, h.superv, h.dtped::date, h.vlvenda,
        h.totpesoliq, h.codcli, h.produto, pd.descricao, h.codfor
    FROM public.data_history AS h
    JOIN public.data_product_details AS pd ON h.produto = pd.code
    WHERE
        (p_pasta IS NULL OR h.observacaofor = p_pasta)
    AND (p_supervisor IS NULL OR h.superv = p_supervisor)
    AND (p_vendedor_nomes IS NULL OR h.nome = ANY(p_vendedor_nomes))
    AND (p_codcli IS NULL OR h.codcli = p_codcli)
    AND (p_fornecedores IS NULL OR h.codfor = ANY(p_fornecedores))
    AND (p_produtos IS NULL OR h.produto = ANY(p_produtos))
    AND (p_cidade IS NULL OR h.cidade = p_cidade)
    AND (p_filial = 'ambas' OR p_filial IS NULL OR h.filial = p_filial)
    AND h.codcli IN (SELECT codigo_cliente FROM ClientBase);
END;
$$;

-- 3.9: Função para o Ecrã 'stock-view' - (Inalterado)
create or replace function get_stock_analysis_data (
  p_pasta TEXT default null,
  p_supervisor TEXT default null,
  p_vendedor_nomes text[] default null,
  p_fornecedores text[] default null,
  p_produtos text[] default null,
  p_rede_group TEXT default null,
  p_redes text[] default null,
  p_cidade TEXT default null,
  p_filial TEXT default 'ambas'
) RETURNS table (
  origem TEXT,
  produto TEXT,
  dtped DATE,
  qtvenda_embalagem_master NUMERIC,
  stock_qty NUMERIC,
  descricao TEXT,
  fornecedor TEXT,
  codfor TEXT,
  dtcadastro DATE
) LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    RETURN QUERY
    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, NULL, p_filial)
    ),
    ProductBase AS (
        SELECT DISTINCT produto FROM public.data_detailed WHERE (p_produtos IS NULL OR produto = ANY(p_produtos))
        UNION
        SELECT DISTINCT produto FROM public.data_history WHERE (p_produtos IS NULL OR produto = ANY(p_produtos))
        UNION
        SELECT DISTINCT product_code FROM public.data_stock WHERE (p_produtos IS NULL OR product_code = ANY(p_produtos))
    )
    -- 1. Bloco de Vendas
    SELECT
        'sale' AS origem,
        v.produto,
        v.dtped::date,
        v.qtvenda_embalagem_master,
        NULL::numeric, NULL::text, NULL::text, NULL::text, NULL::date
    FROM (
        SELECT produto, dtped, qtvenda_embalagem_master, codcli, codfor, filial, observacaofor
        FROM public.data_detailed
        WHERE
            produto IN (SELECT produto FROM ProductBase)
        AND (p_pasta IS NULL OR observacaofor = p_pasta)
        AND codcli IN (SELECT codigo_cliente FROM ClientBase)
        AND (p_fornecedores IS NULL OR codfor = ANY(p_fornecedores))
        AND (p_filial = 'ambas' OR p_filial IS NULL OR filial = p_filial)

        UNION ALL

        SELECT produto, dtped, qtvenda_embalagem_master, codcli, codfor, filial, observacaofor
        FROM public.data_history
        WHERE
            produto IN (SELECT produto FROM ProductBase)
        AND (p_pasta IS NULL OR observacaofor = p_pasta)
        AND codcli IN (SELECT codigo_cliente FROM ClientBase)
        AND (p_fornecedores IS NULL OR codfor = ANY(p_fornecedores))
        AND (p_filial = 'ambas' OR p_filial IS NULL OR filial = p_filial)
    ) AS v

    UNION ALL

    -- 2. Bloco de Estoque
    SELECT
        'stock' AS origem,
        s.product_code,
        NULL::date,
        NULL::numeric,
        s.stock_qty,
        NULL::text,
        NULL::text,
        NULL::text,
        NULL::date
    FROM public.data_stock AS s
    WHERE
        s.product_code IN (SELECT produto FROM ProductBase)
    AND (p_filial = 'ambas' OR p_filial IS NULL OR s.filial = p_filial)

    UNION ALL

    -- 3. Bloco de Detalhes do Produto
    SELECT
        'product' AS origem,
        p.code,
        NULL::date,
        NULL::numeric,
        NULL::numeric,
        p.descricao,
        p.fornecedor,
        p.codfor,
        p.dtcadastro::date
    FROM public.data_product_details AS p
    WHERE
        p.code IN (SELECT produto FROM ProductBase)
    AND (p_fornecedores IS NULL OR p.codfor = ANY(p_fornecedores));
END;
$$;

-- 3.10: Função para os Ecrãs 'innovations'/'coverage' (Inalterado)
create or replace function get_coverage_analysis (
  p_product_codes text[],
  p_include_bonus BOOLEAN,
  p_supervisor TEXT default null,
  p_vendedor_nomes text[] default null,
  p_fornecedores text[] default null,
  p_rede_group TEXT default null,
  p_redes text[] default null,
  p_cidade TEXT default null,
  p_filial TEXT default 'ambas'
) RETURNS table (
  produto TEXT,
  stock_qty NUMERIC,
  current_pdvs BIGINT,
  previous_pdvs BIGINT,
  total_active_clients BIGINT
) LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
DECLARE
    v_base_clientes_count BIGINT;
    v_current_month DATE := date_trunc('month', NOW());
    v_previous_month DATE := date_trunc('month', NOW() - '1 month'::interval);
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    RETURN QUERY
    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, NULL, p_filial)
    ),
    ClientCount AS (
        SELECT COUNT(*) AS total FROM ClientBase
    ),
    CurrentSales AS (
        SELECT DISTINCT codcli, produto
        FROM public.data_detailed
        WHERE dtped >= v_current_month
          AND produto = ANY(p_product_codes)
          AND (vlvenda > 0 OR (p_include_bonus AND vlbonific > 0))
          AND codcli IN (SELECT codigo_cliente FROM ClientBase)
          AND (p_fornecedores IS NULL OR codfor = ANY(p_fornecedores))
    ),
    PreviousSales AS (
        SELECT DISTINCT codcli, produto
        FROM public.data_history
        WHERE dtped >= v_previous_month AND dtped < v_current_month
          AND produto = ANY(p_product_codes)
          AND (vlvenda > 0 OR (p_include_bonus AND vlbonific > 0))
          AND codcli IN (SELECT codigo_cliente FROM ClientBase)
          AND (p_fornecedores IS NULL OR codfor = ANY(p_fornecedores))
    ),
    Stock AS (
        SELECT product_code, SUM(stock_qty) as total_stock
        FROM public.data_stock
        WHERE product_code = ANY(p_product_codes)
          AND (p_filial = 'ambas' OR p_filial IS NULL OR filial = p_filial)
        GROUP BY product_code
    )
    SELECT
        p.produto,
        COALESCE(s.total_stock, 0) AS stock_qty,
        COALESCE(curr.pdvs, 0) AS current_pdvs,
        COALESCE(prev.pdvs, 0) AS previous_pdvs,
        (SELECT total FROM ClientCount) AS total_active_clients
    FROM (
        SELECT unnest(p_product_codes) AS produto
    ) AS p
    LEFT JOIN Stock AS s ON p.produto = s.product_code
    LEFT JOIN (
        SELECT produto, COUNT(DISTINCT codcli) AS pdvs
        FROM CurrentSales GROUP BY produto
    ) AS curr ON p.produto = curr.produto
    LEFT JOIN (
        SELECT produto, COUNT(DISTINCT codcli) AS pdvs
        FROM PreviousSales GROUP BY produto
    ) AS prev ON p.produto = prev.produto;

END;
$$;

-- 3.11: Funções para popular os FILTROS (Dropdowns) - (Inalterado)
create or replace function get_distinct_supervisors () RETURNS table (superv TEXT) LANGUAGE SQL SECURITY DEFINER
set
  search_path = public as $$
    SELECT DISTINCT superv FROM public.data_detailed WHERE superv IS NOT NULL
    UNION SELECT DISTINCT superv FROM public.data_history WHERE superv IS NOT NULL ORDER BY 1;
$$;

create or replace function get_distinct_vendedores (p_supervisor TEXT default null) RETURNS table (nome TEXT) LANGUAGE SQL SECURITY DEFINER
set
  search_path = public as $$
    SELECT DISTINCT nome FROM public.data_detailed
    WHERE nome IS NOT NULL AND (p_supervisor IS NULL OR superv = p_supervisor)
    UNION SELECT DISTINCT nome FROM public.data_history
    WHERE nome IS NOT NULL AND (p_supervisor IS NULL OR superv = p_supervisor) ORDER BY 1;
$$;

create or replace function get_distinct_fornecedores () RETURNS table (codfor TEXT, fornecedor TEXT) LANGUAGE SQL SECURITY DEFINER
set
  search_path = public as $$
    SELECT DISTINCT codfor, fornecedor FROM public.data_detailed
    WHERE codfor IS NOT NULL AND fornecedor IS NOT NULL
    UNION SELECT DISTINCT codfor, fornecedor FROM public.data_history
    WHERE codfor IS NOT NULL AND fornecedor IS NOT NULL ORDER BY 2;
$$;

create or replace function get_distinct_tipos_venda () RETURNS table (tipovenda TEXT) LANGUAGE SQL SECURITY DEFINER
set
  search_path = public as $$
    SELECT DISTINCT tipovenda FROM public.data_detailed
    WHERE tipovenda IS NOT NULL
    UNION SELECT DISTINCT tipovenda FROM public.data_history
    WHERE tipovenda IS NOT NULL ORDER BY 1;
$$;

create or replace function get_distinct_redes () RETURNS table (ramo TEXT) LANGUAGE SQL SECURITY DEFINER
set
  search_path = public as $$
    SELECT DISTINCT ramo FROM public.data_clients
    WHERE ramo IS NOT NULL AND ramo != 'N/A'
    ORDER BY 1;
$$;

-- ETAPA FINAL: Forçar o Supabase a recarregar o esquema
notify pgrst,
'reload schema';
