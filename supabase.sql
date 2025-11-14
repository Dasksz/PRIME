-- =================================================================
-- SCRIPT SQL UNIFICADO V2.5 - PRIME (CORREÇÃO FINAL RPC)
-- OBJETIVO: Corrigir erros de "structure of query does not match"
--           garantindo que todos os CASTs em funções de agregação
--           correspondam exatamente aos tipos de retorno da tabela.
-- =================================================================
-- ETAPA 1: POLÍTICAS DE SEGURANÇA (RLS) - (Inalterado)
-- ... (código inalterado omitido para brevidade) ...

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
        AND ( (p_rede_group IS NULL AND p_supervisor IS NULL AND p_vendedor_nomes IS NULL AND p_cidade IS NULL AND p_codcli IS NULL) OR v.codcli IN (
            SELECT codigo_cliente FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial)
        ))
    )
    SELECT
        -- CORREÇÃO: Adiciona CAST explícito para corresponder aos tipos de retorno
        COALESCE(SUM(v.vlvenda), 0)::numeric AS total_faturamento,
        COALESCE(SUM(v.totpesoliq), 0)::numeric AS total_peso,
        COALESCE(COUNT(DISTINCT v.produto), 0)::bigint AS total_skus,
        COALESCE(COUNT(DISTINCT v.codcli), 0)::bigint AS total_pdvs_positivados,
        COALESCE(v_base_clientes_count, 0)::bigint AS base_clientes_filtro
    FROM VendasFiltradas AS v
    WHERE v.vlvenda > 0 OR v.vlbonific > 0;
END;
$$;


-- 3.2: Gráficos de Barras (Inalterado)
-- ... (código inalterado omitido) ...

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
        -- CORREÇÃO: Garante que a agregação complexa retorne NUMERIC
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

-- 3.4: Tabela de Pedidos Paginada (Inalterado)
-- ... (código inalterado omitido) ...

-- 3.5: Contagem de Pedidos (CORRIGIDO)
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
    SELECT COUNT(*)::bigint -- CORREÇÃO: Adiciona CAST para garantir o tipo
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


-- ETAPA FINAL: Forçar o Supabase a recarregar o esquema
notify pgrst,
'reload schema';
