-- =================================================================
-- SCRIPT SQL UNIFICADO V2.9 - PRIME (CORREÇÃO DE DEPENDÊNCIA DE RLS)
-- OBJETIVO: Reordenar os comandos DROP para remover as políticas de RLS
--           antes de remover as funções das quais elas dependem,
--           resolvendo o erro de dependência.
-- =================================================================

-- ETAPA 1: REMOÇÃO DE POLÍTICAS DE ACESSO (RLS)
-- Removemos TODAS as políticas primeiro para quebrar as dependências com as funções.
DROP POLICY IF EXISTS "Permitir acesso de leitura para usuários aprovados" ON public.data_clients;
DROP POLICY IF EXISTS "Permitir acesso de leitura para usuários aprovados" ON public.data_detailed;
DROP POLICY IF EXISTS "Permitir acesso de leitura para usuários aprovados" ON public.data_history;
DROP POLICY IF EXISTS "Permitir acesso de leitura para usuários aprovados" ON public.data_metadata;
DROP POLICY IF EXISTS "Permitir acesso de leitura para usuários aprovados" ON public.data_orders;
DROP POLICY IF EXISTS "Permitir acesso de leitura para usuários aprovados" ON public.data_product_details;
DROP POLICY IF EXISTS "Permitir que usuários leiam seu próprio perfil" ON public.profiles;
DROP POLICY IF EXISTS "Permitir que usuários aprovados leiam todos os perfis" ON public.profiles;
-- Políticas adicionais encontradas no log de erro do usuário para maior robustez
DROP POLICY IF EXISTS "Permitir acesso total para service_role ou aprovados" ON public.data_active_products;
DROP POLICY IF EXISTS "Permitir acesso total para service_role ou aprovados" ON public.data_clients;
DROP POLICY IF EXISTS "Permitir acesso total para service_role ou aprovados" ON public.data_detailed;
DROP POLICY IF EXISTS "Permitir acesso total para service_role ou aprovados" ON public.data_history;
DROP POLICY IF EXISTS "Permitir acesso total para service_role ou aprovados" ON public.data_innovations;
DROP POLICY IF EXISTS "Permitir acesso total para service_role ou aprovados" ON public.data_metadata;
DROP POLICY IF EXISTS "Permitir acesso total para service_role ou aprovados" ON public.data_orders;
DROP POLICY IF EXISTS "Permitir acesso total para service_role ou aprovados" ON public.data_product_details;
DROP POLICY IF EXISTS "Permitir acesso total para service_role ou aprovados" ON public.data_stock;


-- ETAPA 2: APAGAR FUNÇÕES ANTIGAS (DROP)
-- Agora que as políticas foram removidas, as funções podem ser removidas sem erros.
DROP FUNCTION IF EXISTS public.is_caller_approved();
DROP FUNCTION IF EXISTS get_filtered_client_base(text, text[], text, text[], text, text, text);
DROP FUNCTION IF EXISTS get_main_kpis(text,text,text[],text,text,text,text[],text,text[],text,text);
DROP FUNCTION IF EXISTS get_top_products(text,text,text,text[],text,text,text,text[],text,text[],text,text);
DROP FUNCTION IF EXISTS get_orders_count(text,text,text[],text,text,text,text[],text,text[],text,text);
DROP FUNCTION IF EXISTS get_city_analysis(text,text[],text,text[],text,text);
DROP FUNCTION IF EXISTS public.get_weekly_sales_and_rankings(text,text[]);
DROP FUNCTION IF EXISTS public.get_distinct_supervisors();
DROP FUNCTION IF EXISTS public.get_distinct_vendedores(text);
DROP FUNCTION IF EXISTS public.get_distinct_fornecedores();
DROP FUNCTION IF EXISTS public.get_distinct_tipos_venda();
DROP FUNCTION IF EXISTS public.get_distinct_redes();
DROP FUNCTION IF EXISTS public.get_paginated_orders(integer,integer,text,text,text[],text,text,text,text[],text,text[],text,text);
DROP FUNCTION IF EXISTS public.get_comparison_data(text,text,text[],text[],text[],text,text[],text,text);
DROP FUNCTION IF EXISTS public.get_stock_analysis_data(text,text,text[],text[],text[],text,text);
DROP FUNCTION IF EXISTS public.get_coverage_analysis(text,text[],text[],text,text,text[],boolean);


-- =================================================================
-- ETAPA 3: RECRIAÇÃO DAS FUNÇÕES DE SEGURANÇA E POLÍTICAS DE RLS
-- =================================================================

-- 3.1: Função de Verificação de Acesso
create or replace function public.is_caller_approved()
returns boolean
language plpgsql
security definer
set search_path = public
as $$
begin
  return exists (
    select 1
    from public.profiles
    where id = auth.uid() and status = 'aprovado'
  );
end;
$$;

-- 3.2: Habilitação do Row Level Security (RLS)
-- (É seguro re-habilitar RLS mesmo que já esteja ativo)
alter table public.data_clients enable row level security;
alter table public.data_detailed enable row level security;
alter table public.data_history enable row level security;
alter table public.data_metadata enable row level security;
alter table public.data_orders enable row level security;
alter table public.data_product_details enable row level security;
alter table public.profiles enable row level security;
-- Habilitação para tabelas extras mencionadas no erro
alter table public.data_active_products enable row level security;
alter table public.data_innovations enable row level security;
alter table public.data_stock enable row level security;

-- 3.3: Recriação das Políticas de Acesso (Policies)
CREATE POLICY "Permitir acesso de leitura para usuários aprovados" ON public.data_clients FOR SELECT USING (public.is_caller_approved());
CREATE POLICY "Permitir acesso de leitura para usuários aprovados" ON public.data_detailed FOR SELECT USING (public.is_caller_approved());
CREATE POLICY "Permitir acesso de leitura para usuários aprovados" ON public.data_history FOR SELECT USING (public.is_caller_approved());
CREATE POLICY "Permitir acesso de leitura para usuários aprovados" ON public.data_metadata FOR SELECT USING (public.is_caller_approved());
CREATE POLICY "Permitir acesso de leitura para usuários aprovados" ON public.data_orders FOR SELECT USING (public.is_caller_approved());
CREATE POLICY "Permitir acesso de leitura para usuários aprovados" ON public.data_product_details FOR SELECT USING (public.is_caller_approved());
CREATE POLICY "Permitir que usuários leiam seu próprio perfil" ON public.profiles FOR SELECT USING (auth.uid() = id);
CREATE POLICY "Permitir que usuários aprovados leiam todos os perfis" ON public.profiles FOR SELECT USING (public.is_caller_approved());


-- =================================================================
-- ETAPA 4: RECRIAÇÃO DAS FUNÇÕES DE CÁLCULO (RPC)
-- =================================================================

create or replace function get_filtered_client_base (
  p_supervisor TEXT default null,
  p_vendedor_nomes text[] default null,
  p_rede_group TEXT default null,
  p_redes text[] default null,
  p_cidade TEXT default null,
  p_codcli TEXT default null,
  p_filial TEXT default null
) RETURNS table (codigo_cliente TEXT) LANGUAGE plpgsql SECURITY DEFINER
set search_path = public as $$
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
set search_path = public as $$
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
        AND ( (p_rede_group IS NULL AND p_supervisor IS NULL AND p_vendedor_nomes IS NULL AND p_cidade IS NULL AND p_codcli IS NULL) OR v.codcli IN (
            SELECT codigo_cliente FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial)
        ))
    )
    SELECT
        COALESCE(SUM(v.vlvenda), 0)::numeric AS total_faturamento,
        COALESCE(SUM(v.totpesoliq), 0)::numeric AS total_peso,
        COALESCE(COUNT(DISTINCT v.produto), 0)::bigint AS total_skus,
        COALESCE(COUNT(DISTINCT v.codcli), 0)::bigint AS total_pdvs_positivados,
        COALESCE(v_base_clientes_count, 0)::bigint AS base_clientes_filtro
    FROM VendasFiltradas AS v
    WHERE v.vlvenda > 0 OR v.vlbonific > 0;
END;
$$;

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
set search_path = public as $$
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
        SUM(
            CASE
                WHEN p_metric = 'faturamento' THEN v.vlvenda
                WHEN p_metric = 'peso' THEN v.totpesoliq
                ELSE 0::numeric
            END
        )::numeric AS valor_metrica
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
    AND ( (p_rede_group IS NULL AND p_supervisor IS NULL AND p_vendedor_nomes IS NULL AND p_cidade IS NULL AND p_codcli IS NULL) OR v.codcli IN (SELECT codigo_cliente FROM ClientBase) )
    GROUP BY
        v.produto, pd.descricao
    ORDER BY
        valor_metrica DESC
    LIMIT 10;
END;
$$;

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
) RETURNS TABLE(total_count BIGINT) LANGUAGE plpgsql SECURITY DEFINER
set search_path = public as $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    RETURN QUERY
    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial)
    )
    SELECT COUNT(*)::bigint
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
    AND ( (p_rede_group IS NULL AND p_supervisor IS NULL AND p_vendedor_nomes IS NULL AND p_cidade IS NULL AND p_codcli IS NULL) OR o.codcli IN (SELECT codigo_cliente FROM ClientBase) );
END;
$$;

create or replace function get_city_analysis(
    p_supervisor text DEFAULT NULL,
    p_vendedor_nomes text[] DEFAULT NULL,
    p_rede_group text DEFAULT NULL,
    p_redes text[] DEFAULT NULL,
    p_cidade text DEFAULT NULL,
    p_codcli text DEFAULT NULL
) 
RETURNS TABLE (
    tipo_analise text,
    group_name text,
    total_faturamento numeric,
    status_cliente text,
    codigo_cliente text,
    fantasia text,
    cidade text,
    bairro text,
    ultimacompra date,
    rca1 text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public AS $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    RETURN QUERY
    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, NULL)
    ),
    SalesData AS (
        SELECT
            codcli,
            SUM(vlvenda) AS faturamento
        FROM public.data_detailed
        WHERE codcli IN (SELECT codigo_cliente FROM ClientBase)
        GROUP BY codcli
    )
    -- Gráfico: Top 10 Clientes na Cidade/Filtro
    SELECT
        'chart'::text AS tipo_analise,
        c.fantasia::text AS group_name,
        COALESCE(s.faturamento, 0)::numeric AS total_faturamento,
        NULL::text, NULL::text, NULL::text, NULL::text, NULL::text, NULL::date, NULL::text
    FROM public.data_clients c
    LEFT JOIN SalesData s ON c.codigo_cliente = s.codcli
    WHERE c.codigo_cliente IN (SELECT codigo_cliente FROM ClientBase)
    ORDER BY total_faturamento DESC
    LIMIT 10

    UNION ALL

    -- Lista de Clientes para as Tabelas
    SELECT
        'client_list'::text AS tipo_analise,
        c.fantasia::text AS group_name,
        COALESCE(s.faturamento, 0)::numeric AS total_faturamento,
        CASE
            WHEN c.dtinclusao >= (NOW() - INTERVAL '90 days') THEN 'novo'
            WHEN c.ultimacompra < (NOW() - INTERVAL '45 days') THEN 'inativo'
            ELSE 'ativo'
        END::text AS status_cliente,
        c.codigo_cliente::text,
        c.fantasia::text,
        c.cidade::text,
        c.bairro::text,
        c.ultimacompra::date,
        c.rca1::text
    FROM public.data_clients c
    LEFT JOIN SalesData s ON c.codigo_cliente = s.codcli
    WHERE c.codigo_cliente IN (SELECT codigo_cliente FROM ClientBase);
END;
$$;

CREATE OR REPLACE FUNCTION public.get_weekly_sales_and_rankings(
    p_pasta text DEFAULT NULL,
    p_supervisores text[] DEFAULT NULL
)
RETURNS TABLE(
    tipo_dado text,
    group_name text,
    total_valor numeric,
    dia_semana text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET SEARCH_PATH = public AS $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    RETURN QUERY
    -- 1. Vendas da semana atual por dia
    (WITH VendasDaSemana AS (
        SELECT
            v.nome,
            v.superv,
            v.vlvenda,
            to_char(v.dtped AT TIME ZONE 'UTC', 'Day') AS dia_semana_nome,
            extract(isodow from v.dtped AT TIME ZONE 'UTC') AS dia_semana_num
        FROM public.data_detailed v
        WHERE
            v.dtped >= date_trunc('week', NOW() AT TIME ZONE 'UTC')
        AND (p_pasta IS NULL OR v.observacaofor = p_pasta)
        AND (p_supervisores IS NULL OR v.superv = ANY(p_supervisores))
    )
    SELECT
        'venda_semanal'::text,
        s.superv::text,
        SUM(s.vlvenda)::numeric,
        trim(s.dia_semana_nome)::text
    FROM VendasDaSemana s
    GROUP BY s.superv, s.dia_semana_nome, s.dia_semana_num
    ORDER BY s.dia_semana_num)

    UNION ALL

    -- 2. Ranking de Positivação
    (SELECT
        'rank_positivacao'::text,
        v.superv::text,
        COUNT(DISTINCT v.codcli)::numeric AS total_valor,
        NULL::text
    FROM public.data_detailed v
    WHERE
        v.dtped >= date_trunc('month', NOW() AT TIME ZONE 'UTC')
    AND (p_pasta IS NULL OR v.observacaofor = p_pasta)
    AND (p_supervisores IS NULL OR v.superv = ANY(p_supervisores))
    GROUP BY v.superv
    ORDER BY total_valor DESC
    LIMIT 5)

    UNION ALL

    -- 3. Ranking Top Sellers (Faturamento)
    (SELECT
        'rank_topsellers'::text,
        v.nome::text,
        SUM(v.vlvenda)::numeric AS total_valor,
        NULL::text
    FROM public.data_detailed v
    WHERE
        v.dtped >= date_trunc('month', NOW() AT TIME ZONE 'UTC')
    AND (p_pasta IS NULL OR v.observacaofor = p_pasta)
    AND (p_supervisores IS NULL OR v.superv = ANY(p_supervisores))
    GROUP BY v.nome
    ORDER BY total_valor DESC
    LIMIT 10)

    UNION ALL

    -- 4. Ranking de Mix de Produto
    (SELECT
        'rank_mix'::text,
        v.superv::text,
        (COUNT(DISTINCT v.produto)::decimal / COUNT(DISTINCT v.codcli))::numeric AS total_valor,
        NULL::text
    FROM public.data_detailed v
    WHERE
        v.dtped >= date_trunc('month', NOW() AT TIME ZONE 'UTC')
    AND (p_pasta IS NULL OR v.observacaofor = p_pasta)
    AND (p_supervisores IS NULL OR v.superv = ANY(p_supervisores))
    GROUP BY v.superv
    HAVING COUNT(DISTINCT v.codcli) > 0
    ORDER BY total_valor DESC
    LIMIT 5);
END;
$$;

CREATE OR REPLACE FUNCTION public.get_distinct_supervisors()
RETURNS TABLE(superv text)
LANGUAGE plpgsql
SECURITY DEFINER
SET SEARCH_PATH = public AS $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;
    RETURN QUERY SELECT DISTINCT s.superv FROM (
        SELECT superv FROM data_detailed WHERE superv IS NOT NULL
        UNION
        SELECT superv FROM data_history WHERE superv IS NOT NULL
    ) AS s ORDER BY s.superv;
END;
$$;

CREATE OR REPLACE FUNCTION public.get_distinct_vendedores(p_supervisor text DEFAULT NULL)
RETURNS TABLE(nome text)
LANGUAGE plpgsql
SECURITY DEFINER
SET SEARCH_PATH = public AS $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;
    RETURN QUERY SELECT DISTINCT s.nome FROM (
        SELECT nome FROM data_detailed WHERE nome IS NOT NULL AND (p_supervisor IS NULL OR superv = p_supervisor)
        UNION
        SELECT nome FROM data_history WHERE nome IS NOT NULL AND (p_supervisor IS NULL OR superv = p_supervisor)
    ) AS s ORDER BY s.nome;
END;
$$;

CREATE OR REPLACE FUNCTION public.get_distinct_fornecedores()
RETURNS TABLE(codfor text, fornecedor text)
LANGUAGE plpgsql
SECURITY DEFINER
SET SEARCH_PATH = public AS $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;
    RETURN QUERY SELECT DISTINCT codfor::text, fornecedor::text FROM public.data_product_details WHERE fornecedor IS NOT NULL ORDER BY fornecedor;
END;
$$;

CREATE OR REPLACE FUNCTION public.get_distinct_tipos_venda()
RETURNS TABLE(tipovenda text)
LANGUAGE plpgsql
SECURITY DEFINER
SET SEARCH_PATH = public AS $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;
    RETURN QUERY SELECT DISTINCT s.tipovenda FROM (
        SELECT tipovenda FROM data_detailed WHERE tipovenda IS NOT NULL
        UNION
        SELECT tipovenda FROM data_history WHERE tipovenda IS NOT NULL
    ) AS s ORDER BY s.tipovenda;
END;
$$;

CREATE OR REPLACE FUNCTION public.get_distinct_redes()
RETURNS TABLE(rede text)
LANGUAGE plpgsql
SECURITY DEFINER
SET SEARCH_PATH = public AS $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;
    RETURN QUERY SELECT DISTINCT ramo AS rede FROM public.data_clients WHERE ramo IS NOT NULL AND ramo <> 'N/A' ORDER BY ramo;
END;
$$;

CREATE OR REPLACE FUNCTION public.get_paginated_orders(
    p_page_number integer,
    p_page_size integer,
    p_pasta text DEFAULT NULL,
    p_supervisor text DEFAULT NULL,
    p_vendedor_nomes text[] DEFAULT NULL,
    p_codcli text DEFAULT NULL,
    p_posicao text DEFAULT NULL,
    p_codfor text DEFAULT NULL,
    p_tipos_venda text[] DEFAULT NULL,
    p_rede_group text DEFAULT NULL,
    p_redes text[] DEFAULT NULL,
    p_cidade text DEFAULT NULL,
    p_filial text DEFAULT 'ambas'
)
RETURNS TABLE(
    pedido text, codcli text, nome text, fornecedores_list text[], dtped date, dtfat date, totpesoliq numeric, vltotal numeric, posicao text
)
LANGUAGE plpgsql SECURITY DEFINER SET SEARCH_PATH = public AS $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;
    RETURN QUERY
    WITH ClientBase AS (
        SELECT codigo_cliente FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial)
    )
    SELECT
        o.numped::text AS pedido,
        o.codcli::text,
        o.nome::text,
        o.fornecedores_list,
        o.dtped::date,
        o.dtfat::date,
        o.totpesoliq,
        o.vltotal,
        o.posicao::text
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
    AND (o.codcli IN (SELECT codigo_cliente FROM ClientBase))
    ORDER BY o.dtped DESC
    LIMIT p_page_size
    OFFSET (p_page_number - 1) * p_page_size;
END;
$$;

CREATE OR REPLACE FUNCTION public.get_comparison_data(
    p_pasta text DEFAULT NULL,
    p_supervisor text DEFAULT NULL,
    p_vendedor_nomes text[] DEFAULT NULL,
    p_fornecedores text[] DEFAULT NULL,
    p_produtos text[] DEFAULT NULL,
    p_rede_group text DEFAULT NULL,
    p_redes text[] DEFAULT NULL,
    p_cidade text DEFAULT NULL,
    p_filial text DEFAULT 'ambas'
)
RETURNS TABLE(
    origem text, mes_historico integer, supervisor text, codcli text, total_faturamento numeric, total_peso numeric
)
LANGUAGE plpgsql SECURITY DEFINER SET SEARCH_PATH = public AS $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;
    RETURN QUERY
    WITH ClientBase AS (
        SELECT codigo_cliente FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, NULL, p_filial)
    )
    SELECT
        'current'::text as origem,
        NULL::integer as mes_historico,
        d.superv,
        d.codcli,
        SUM(d.vlvenda)::numeric as total_faturamento,
        SUM(d.totpesoliq)::numeric as total_peso
    FROM data_detailed d
    WHERE d.codcli IN (SELECT codigo_cliente FROM ClientBase)
    AND (p_pasta IS NULL OR d.observacaofor = p_pasta)
    AND (p_fornecedores IS NULL OR d.fornecedor = ANY(p_fornecedores))
    AND (p_produtos IS NULL OR d.produto = ANY(p_produtos))
    GROUP BY d.superv, d.codcli

    UNION ALL

    SELECT
        'history'::text as origem,
        h.mes_relativo as mes_historico,
        h.superv,
        h.codcli,
        SUM(h.vlvenda)::numeric as total_faturamento,
        SUM(h.totpesoliq)::numeric as total_peso
    FROM data_history h
    WHERE h.codcli IN (SELECT codigo_cliente FROM ClientBase)
    AND (p_pasta IS NULL OR h.observacaofor = p_pasta)
    AND (p_fornecedores IS NULL OR h.fornecedor = ANY(p_fornecedores))
    AND (p_produtos IS NULL OR h.produto = ANY(p_produtos))
    GROUP BY h.superv, h.codcli, h.mes_relativo;
END;
$$;

CREATE OR REPLACE FUNCTION public.get_stock_analysis_data(
    p_pasta text DEFAULT NULL,
    p_supervisor text DEFAULT NULL,
    p_vendedor_nomes text[] DEFAULT NULL,
    p_fornecedores text[] DEFAULT NULL,
    p_produtos text[] DEFAULT NULL,
    p_cidade text DEFAULT NULL,
    p_filial text DEFAULT 'ambas'
)
RETURNS TABLE (
    produto_descricao text, fornecedor text, estoque_atual_cx numeric, venda_media_mensal_cx numeric, media_diaria_cx numeric, tendencia_dias numeric,
    status_produto text, venda_atual_cx numeric, media_trimestre_cx numeric, variacao numeric
)
LANGUAGE plpgsql SECURITY DEFINER SET SEARCH_PATH = public AS $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;
    RETURN QUERY
    SELECT
        p.descricao::text as produto_descricao,
        p.fornecedor::text,
        p.estfisico::numeric as estoque_atual_cx,
        (SELECT AVG(v.total_venda) FROM (SELECT SUM(h.qtvenda) as total_venda FROM data_history h WHERE h.produto = p.code GROUP BY h.mes_relativo) v)::numeric as venda_media_mensal_cx,
        (SELECT SUM(d.qtvenda) / 21.0 FROM data_detailed d WHERE d.produto = p.code)::numeric as media_diaria_cx,
        (p.estfisico / NULLIF((SELECT SUM(d.qtvenda) / 21.0 FROM data_detailed d WHERE d.produto = p.code), 0))::numeric as tendencia_dias,
        'n/a'::text as status_produto,
        0::numeric as venda_atual_cx,
        0::numeric as media_trimestre_cx,
        0::numeric as variacao
    FROM data_product_details p
    WHERE (p_fornecedores IS NULL OR p.fornecedor = ANY(p_fornecedores))
    AND (p_produtos IS NULL OR p.code = ANY(p_produtos));
END;
$$;

CREATE OR REPLACE FUNCTION public.get_coverage_analysis(
    p_supervisor text DEFAULT NULL,
    p_vendedor_nomes text[] DEFAULT NULL,
    p_fornecedores text[] DEFAULT NULL,
    p_cidade text DEFAULT NULL,
    p_filial text DEFAULT 'ambas',
    p_product_codes text[] DEFAULT NULL,
    p_include_bonus boolean DEFAULT true
)
RETURNS TABLE (
    produto_descricao text, estoque_cx numeric, cobertura_estoque_dias numeric, pdvs_mes_anterior bigint, pdvs_mes_atual bigint, cobertura_pdvs numeric,
    cobertura_anterior numeric, cobertura_atual numeric
)
LANGUAGE plpgsql SECURITY DEFINER SET SEARCH_PATH = public AS $$
BEGIN
     IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;
    RETURN QUERY
    SELECT
        p.descricao::text as produto_descricao,
        p.estfisico::numeric as estoque_cx,
        0::numeric as cobertura_estoque_dias,
        (SELECT COUNT(DISTINCT h.codcli) FROM data_history h WHERE h.produto = p.code)::bigint as pdvs_mes_anterior,
        (SELECT COUNT(DISTINCT d.codcli) FROM data_detailed d WHERE d.produto = p.code)::bigint as pdvs_mes_atual,
        0::numeric as cobertura_pdvs,
        0::numeric as cobertura_anterior,
        0::numeric as cobertura_atual
    FROM data_product_details p
    WHERE p.code = ANY(p_product_codes);
END;
$$;


-- =================================================================
-- ETAPA 5: CONCESSÃO DE PERMISSÕES (GRANT)
-- =================================================================

GRANT EXECUTE ON FUNCTION public.get_distinct_supervisors() TO anon;
GRANT EXECUTE ON FUNCTION public.get_distinct_vendedores(text) TO anon;
GRANT EXECUTE ON FUNCTION public.get_distinct_fornecedores() TO anon;
GRANT EXECUTE ON FUNCTION public.get_distinct_tipos_venda() TO anon;
GRANT EXECUTE ON FUNCTION public.get_distinct_redes() TO anon;
GRANT EXECUTE ON FUNCTION public.get_main_kpis(text,text,text[],text,text,text,text[],text,text[],text,text) TO anon;
GRANT EXECUTE ON FUNCTION public.get_top_products(text,text,text,text[],text,text,text,text[],text,text[],text,text) TO anon;
GRANT EXECUTE ON FUNCTION public.get_orders_count(text,text,text[],text,text,text,text[],text,text[],text,text) TO anon;
GRANT EXECUTE ON FUNCTION public.get_paginated_orders(integer,integer,text,text,text[],text,text,text,text[],text,text[],text,text) TO anon;
GRANT EXECUTE ON FUNCTION public.get_city_analysis(text,text[],text,text[],text,text) TO anon;
GRANT EXECUTE ON FUNCTION public.get_weekly_sales_and_rankings(text,text[]) TO anon;
GRANT EXECUTE ON FUNCTION public.get_comparison_data(text,text,text[],text[],text[],text,text[],text,text) TO anon;
GRANT EXECUTE ON FUNCTION public.get_stock_analysis_data(text,text,text[],text[],text[],text,text) TO anon;
GRANT EXECUTE ON FUNCTION public.get_coverage_analysis(text,text[],text[],text,text,text[],boolean) TO anon;

-- ETAPA FINAL: Forçar o Supabase a recarregar o esquema
NOTIFY pgrst, 'reload schema';
